# Using pco effectively in QuestDB

pco is QuestDB's fitted numeric codec, opt-in per column via `PARQUET(PCO)`. It
applies **only on the native -> Parquet conversion path** (converted partitions),
never to the native `.d` hot store, and a pco column is readable only by QuestDB.
Eligible types: `FLOAT`, `DOUBLE`, `SHORT`, `INT`, `LONG`, `TIMESTAMP`, `DATE`.

This guide is the practical model for *when pco helps and how to feed it*. It is
general; the numbers cited are illustrative measurements, not a dataset report.

## The one mental model

pco's compressed size tracks the **entropy of the values** -- after it applies
its own automatic delta and "common-multiple" (int-mult) detection and bins the
residuals. It does **not** track the storage width. Two corollaries drive
everything below:

1. The same values stored as `i16`, `i32`, or `i64` produce a **byte-for-byte
   identical** pco blob. (Measured: a millivolt column compressed to 0.122
   bytes/value as LONG, INT, and SHORT alike.)
2. The lever you *do* control is how many distinct values there are -- precision
   and noise -- not how many bytes each nominally occupies.

## Rule 1 -- Do not downcast integers for Parquet density. SHORT earns nothing here.

For the **Parquet layer**, narrowing an integer column (LONG -> INT -> SHORT)
does not shrink the file:

- pco is width-agnostic (corollary 1), so the compressed payload is identical.
- Even *uncompressed*, Parquet has no 16-bit integer: QuestDB writes `SHORT`,
  `BYTE`, and `CHAR` as the Parquet `INT32` physical type. So a SHORT column is
  already 4 bytes per value in Parquet before pco, same as INT.

So for Parquet, **SHORT (and BYTE) offer no size advantage over INT** -- with or
without pco. Their genuine 2-byte / 1-byte footprint exists **only in the native
`.d` files** (the hot, not-yet-converted partitions). If a partition's lifetime
is mostly Parquet, pick the integer type for *semantics*, not for width.

Practical reading: choosing SHORT specifically to make Parquet smaller is a
no-op. Choose SHORT only when the native `.d` footprint matters (recent data,
high ingest rate) and the column has no NULLs and a safe 16-bit range.

> Why SHORT pco support still exists: a column that *is* SHORT (chosen for the
> `.d` win) must still be encodable with pco when its partition converts -- and
> it now is. The feature makes SHORT *work* with pco; it does not make SHORT a
> way to *get* smaller pco.

## Rule 2 -- The real lever is precision/entropy, not width

To make a pco column smaller, reduce the information it carries:

- **Round floats to a lossless fixed-point grid.** If a value is only meaningful
  to 0.01, store `round(x*100)` as an integer -- pco then sees a low-entropy
  integer stream instead of noisy IEEE mantissas. (Measured: a near-integer
  "float" column dropped from ~1.0 bytes/value as f32 to ~0.03 as a *whole-unit*
  int -- but most of that win is dropping precision, not the type change; see
  Rule 3 for the split.)
- **Drop noise bits you do not need** via `PARQUET(PCO, LOSSY(n))` for FLOAT /
  DOUBLE (keep the top `n` mantissa bits). This is the precision lever made
  explicit; it trades a bounded relative error for density.
- **For wide-dynamic-range positive columns, use a log grid, not a linear one.**
  A linear `round(x*100)` grid needs huge integers when values span many orders
  of magnitude (e.g. prices from 1e-6 to 1e6), which defeats the purpose.
  Storing `round(ln(x) * scale)` as a LONG instead holds a constant *relative*
  error across the whole range in a compact integer. On real price data this
  beat both `LOSSY(n)` and a linear grid by ~15-40% (and ~2x at f32-grade
  precision). Positive values only: zeros and negatives have no real log, so it
  cannot be used on signed columns (a signed current, a signed trade quantity).
  It is a client-side pre-transform (decode with `exp`), lossy with a bounded
  relative error, and -- unlike the delta in Rule 4 -- legitimately additive,
  because the log is nonlinear and pco does not do it for you. Not a shipped DDL
  option today; do it at ingestion if it pays for your data.

Precision reduction shrinks pco because it collapses distinct values. Width
reduction does not, because pco never charged you for the width.

**But precision reduction is not monotonic -- measure it.** Rounding usually
shrinks pco, but on a signed or noisy column it can *grow* the blob: the
rounding perturbs the values pco's delta / int-mult detection keys on. Measured:
a signed, oscillating current column went from 1.090 bytes/value lossless to
1.306 at `LOSSY` keep=12 -- rounding made it *bigger*. Always compare lossless
pco against the rounded variant before committing to `LOSSY`; lossless
frequently wins on signed or noisy data (and the log grid above is unusable
there anyway).

## Rule 3 -- Store integer-valued floats as integers

Columns declared `FLOAT`/`DOUBLE` that actually hold whole numbers or fixed
decimals (counts, flags, fixed-point sensor readings) compress better as
`INT`/`LONG`, losslessly. Verify losslessness (`round(x*scale) / scale == x`)
before changing the type. But be clear about *why* and *how much*, because it is
easy to overstate.

**Why a "nice" decimal does not compress well as a float.** A value like
`104.910` has no exact binary representation -- `0.001` is a non-terminating
binary fraction -- so IEEE-754 stores `104.90999...` with pseudo-random low
mantissa bits encoding the rounding. pco faithfully preserves those bits, and
they cost entropy: the decimal "niceness" is **invisible to a binary codec**.
pco's float-multiplier mode is supposed to factor out a common base like
`0.001`, but it cannot when the base is not exactly representable, and `f32`'s
short mantissa actively breaks the grid at large magnitude (measured: at ~1.6e6
the stored `f32` misses the milli-grid by up to 0.5). A wide dynamic range makes
it worse -- the values spread across many binary exponents, each its own latent.
Counter-intuitively, **`f64` can compress such data worse than `f32`** (measured
6.75 vs 3.12 bytes/value on wide-range `k/1000`): more mantissa bits give the
non-terminating binary more room to fill with noise.

**Two separate levers -- do not conflate them.** Converting a fractional float to
a scaled int mixes two effects:

- *Representation* (float -> scaled int at the **same** precision): exact integer
  values and one clean latent stream instead of sign/exponent/mantissa. Real but
  **modest** -- measured `f32` 1.012 -> `int(x1000)` 0.719 bytes/value, ~1.4x.
- *Precision* (storing **fewer** low-order digits because they are noise): the
  large win, but **lossy** -- `int(x1000)` 0.719 -> whole-unit `int` 0.026, ~27x.
  Only valid if those digits really are noise (often true at high magnitude,
  where `f32` could not hold them anyway).

So: type the column as `INT`/`LONG` for the lossless representation gain, then
decide the *scale* (how many decimals to keep) as a precision call per Rule 2 --
and measure, since on signed/noisy columns rounding can grow the blob. The
dramatic ratios come from the precision decision, not the type change.

## Rule 4 -- Let pco do the delta; don't pre-transform

pco auto-deltas and detects common multiples. A manual delta pass before pco
earns essentially nothing (measured: delta+pco within ~1% of bare pco). Feeding
it "pre-helped" data adds code and risk for no gain. Hand pco the raw values.

## Rule 5 -- NULL and the no-null types

`SHORT`, `BYTE`, `CHAR` have **no NULL** in QuestDB -- a missing value reads back
as 0. A column with real NULLs must use a type that has a NULL sentinel
(`INT` = i32::MIN, `LONG` = i64::MIN, floats = NaN), regardless of range or
compression. pco compresses the repeated NULL sentinel almost for free, so a
mostly-null INT column is still tiny -- you do not need SHORT to make it small.

This often decides SHORT-vs-INT on its own: if the column can be null, it is INT.

## Rule 6 -- Sorted, monotonic, regular, or low-cardinality -> near zero

The designated timestamp, any monotonic counter, a regular sampling grid, or a
near-constant column all collapse to ~0 bytes/value under pco's delta + int-mult.
Two consequences:

- These columns are already free; there is nothing to optimize.
- **Reduced timestamp precision (ms/s) only helps when the timestamps carry
  sub-second entropy** -- irregular, microsecond-resolution event times. A
  downsampled or whole-second grid sees no benefit (measured: us = ms = s,
  identical, on a whole-second multi-device grid). Do not reach for a
  millisecond timestamp to shrink a regular series; it is already ~0.

## Rule 7 -- No extra compression on top

QuestDB writes pco pages **uncompressed**: pco output is already entropy-coded,
so a Parquet codec (zstd/snappy) on top wastes CPU and can grow the page. Do not
pair `PARQUET(PCO, ZSTD(...))`.

## Quick recipe

1. Numeric column: pick the smallest type that holds the range **with headroom
   for glitches/outliers** and supports NULL if the column can be null. For
   Parquet size this is a semantic choice, not a compression one -- don't pick
   SHORT *for* pco.
2. Float that is integer-valued or fixed-precision: store it as `INT`/`LONG` or
   lossless fixed-point, then `PARQUET(PCO)`.
3. Float that is genuinely fractional and tolerant: `PARQUET(PCO, LOSSY(n))` --
   but verify it actually shrinks (on signed/noisy columns lossless can win). For
   a wide-range positive column, a client-side `round(ln(x)*scale)` log grid
   beats `LOSSY` and a linear grid.
4. Timestamp / monotonic / sorted: leave at native precision, `PARQUET(PCO)` (or
   the default delta). Don't reduce precision unless you have measured
   sub-second entropy.
5. Never stack a Parquet compressor on a pco column.

## What changes between `.d` and Parquet (summary)

| concern                     | native `.d`            | Parquet + pco                 |
|-----------------------------|------------------------|-------------------------------|
| SHORT/BYTE physical width   | 2 B / 1 B (real win)   | INT32 / always 4 B (no win)   |
| integer width vs size       | narrower = smaller     | width-agnostic (same size)    |
| what shrinks the column     | type width             | value entropy / precision     |
| NULL on SHORT/BYTE/CHAR     | none (reads 0)         | none (reads 0)                |

The short version: **for Parquet, choose column types by range and NULL
semantics; choose pco settings by precision. Width is the `.d` story, entropy is
the pco story.**
