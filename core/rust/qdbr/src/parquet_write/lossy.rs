//! Lossy precision reduction for floating-point columns ("bit grooming").
//!
//! Tier A of the lossy float compression design (see
//! `docs/lossy-float-compression.md`). Rounding a value to keep only the top
//! `keep` mantissa bits zeroes the low-order bits the user has declared
//! irrelevant. The result stays a valid IEEE-754 value, so nothing on the read
//! path changes; the now-constant low bytes simply compress away once the
//! column is byte-stream-split and run through the compressor.
//!
//! Rounding is round-to-nearest, ties-to-even, applied to the magnitude so it is
//! sign-symmetric. The maximum relative error is `2^-(keep + 1)`. NaN, infinity
//! and signed zero pass through unchanged so null handling (NaN sentinel) and
//! exact zeros are preserved.

/// Number of explicitly stored mantissa bits in an IEEE-754 `f64` / `f32`.
const F64_MANTISSA_BITS: u32 = 52;
const F32_MANTISSA_BITS: u32 = 23;

const F64_SIGN_MASK: u64 = 1 << 63;
const F32_SIGN_MASK: u32 = 1 << 31;

/// Round `x` to keep only the top `keep` mantissa bits (round half to even).
/// `keep >= 52` (or a non-finite / zero input) returns `x` unchanged.
#[inline]
pub fn round_f64(x: f64, keep: u32) -> f64 {
    if keep >= F64_MANTISSA_BITS || !x.is_finite() || x == 0.0 {
        return x;
    }
    let drop = F64_MANTISSA_BITS - keep;
    let bits = x.to_bits();
    let sign = bits & F64_SIGN_MASK;
    let mag = bits & !F64_SIGN_MASK;
    let round_bit = 1u64 << (drop - 1);
    let low_mask = (1u64 << drop) - 1;
    let lower = mag & low_mask;
    let mut hi = mag & !low_mask;
    // Round up on a strict majority, or on an exact tie when the kept bit is odd.
    if lower > round_bit || (lower == round_bit && (mag >> drop) & 1 == 1) {
        // A carry here propagates into the exponent (correct) and, at the very
        // top of the range, can saturate to infinity. That is the right rounding
        // of a value already at the edge of the representable range.
        hi += 1u64 << drop;
    }
    f64::from_bits(sign | hi)
}

/// Round `x` to keep only the top `keep` mantissa bits (round half to even).
/// `keep >= 23` (or a non-finite / zero input) returns `x` unchanged.
#[inline]
pub fn round_f32(x: f32, keep: u32) -> f32 {
    if keep >= F32_MANTISSA_BITS || !x.is_finite() || x == 0.0 {
        return x;
    }
    let drop = F32_MANTISSA_BITS - keep;
    let bits = x.to_bits();
    let sign = bits & F32_SIGN_MASK;
    let mag = bits & !F32_SIGN_MASK;
    let round_bit = 1u32 << (drop - 1);
    let low_mask = (1u32 << drop) - 1;
    let lower = mag & low_mask;
    let mut hi = mag & !low_mask;
    if lower > round_bit || (lower == round_bit && (mag >> drop) & 1 == 1) {
        hi += 1u32 << drop;
    }
    f32::from_bits(sign | hi)
}

/// Round a little-endian `f64` value buffer in place into a fresh buffer,
/// keeping the top `keep` mantissa bits of each value. The input is the raw
/// column data (8 bytes per value); any trailing partial value is copied
/// verbatim, which never happens for well-formed fixed-width columns.
pub fn round_f64_bytes(src: &[u8], keep: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len());
    let mut buf = [0u8; 8];
    let mut chunks = src.chunks_exact(8);
    for chunk in &mut chunks {
        buf.copy_from_slice(chunk);
        out.extend_from_slice(&round_f64(f64::from_le_bytes(buf), keep).to_le_bytes());
    }
    out.extend_from_slice(chunks.remainder());
    out
}

/// Round a little-endian `f32` value buffer (4 bytes per value). See
/// [`round_f64_bytes`].
pub fn round_f32_bytes(src: &[u8], keep: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len());
    let mut buf = [0u8; 4];
    let mut chunks = src.chunks_exact(4);
    for chunk in &mut chunks {
        buf.copy_from_slice(chunk);
        out.extend_from_slice(&round_f32(f32::from_le_bytes(buf), keep).to_le_bytes());
    }
    out.extend_from_slice(chunks.remainder());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_f64_keeps_full_precision_when_keep_is_max() {
        for &x in &[1.0, -3.5, 1.234_567_891_234_567e10, f64::MIN_POSITIVE] {
            assert_eq!(round_f64(x, 52), x);
            assert_eq!(round_f64(x, 99), x);
        }
    }

    #[test]
    fn round_f64_preserves_non_finite_and_zero() {
        assert!(round_f64(f64::NAN, 10).is_nan());
        assert_eq!(round_f64(f64::INFINITY, 10), f64::INFINITY);
        assert_eq!(round_f64(f64::NEG_INFINITY, 10), f64::NEG_INFINITY);
        // Signed zero is preserved, including the sign bit.
        assert_eq!(round_f64(0.0, 10).to_bits(), 0.0f64.to_bits());
        assert_eq!(round_f64(-0.0, 10).to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn round_f64_zeroes_dropped_mantissa_bits() {
        // Keeping 10 bits must leave the low 42 mantissa bits clear.
        let keep = 10u32;
        let drop = F64_MANTISSA_BITS - keep;
        for i in 0..1000u64 {
            let x = (i as f64) * 0.123_456_789 - 50.0;
            if x == 0.0 {
                continue;
            }
            let r = round_f64(x, keep);
            let low = r.to_bits() & ((1u64 << drop) - 1);
            assert_eq!(low, 0, "low mantissa bits not cleared for x={x}, r={r}");
        }
    }

    #[test]
    fn round_f64_respects_relative_error_bound() {
        for keep in [4u32, 8, 16, 23, 40] {
            // Relative error of round-to-nearest with `keep` kept bits is at most
            // 2^-(keep+1). Allow a hair of slack for the binade-boundary case.
            let bound = 2f64.powi(-(keep as i32 + 1)) * 1.000_001;
            for i in 1..2000u64 {
                let x = (i as f64).powf(1.3) * 7.0e-3 * if i % 2 == 0 { -1.0 } else { 1.0 };
                let r = round_f64(x, keep);
                let rel = ((r - x) / x).abs();
                assert!(
                    rel <= bound,
                    "keep={keep} x={x} r={r} rel={rel} bound={bound}"
                );
            }
        }
    }

    #[test]
    fn round_f64_ties_to_even() {
        // With one kept mantissa bit, the representable neighbours of 1.75 are
        // 1.5 (kept bit odd) and 2.0 (kept bit even). 1.75 is the exact midpoint,
        // so ties-to-even must pick 2.0.
        assert_eq!(round_f64(1.75, 1), 2.0);
        // 1.25 is the midpoint of 1.0 (even) and 1.5 (odd); ties-to-even picks 1.0.
        assert_eq!(round_f64(1.25, 1), 1.0);
        // Sign symmetry: the same ties resolve on the magnitude.
        assert_eq!(round_f64(-1.75, 1), -2.0);
        assert_eq!(round_f64(-1.25, 1), -1.0);
    }

    #[test]
    fn round_f32_respects_relative_error_bound() {
        for keep in [4u32, 8, 12, 20] {
            let bound = 2f32.powi(-(keep as i32 + 1)) * 1.0001;
            for i in 1..2000u32 {
                let x = (i as f32).powf(1.2) * 3.0e-2 * if i % 2 == 0 { -1.0 } else { 1.0 };
                let r = round_f32(x, keep);
                let rel = ((r - x) / x).abs();
                assert!(rel <= bound, "keep={keep} x={x} r={r} rel={rel}");
            }
        }
    }

    #[test]
    fn round_f64_bytes_matches_scalar() {
        let values: Vec<f64> = vec![1.0, -3.5, 1234.5678, f64::NAN, -0.0, 9.87e12, 1e-9];
        let mut src = Vec::new();
        for v in &values {
            src.extend_from_slice(&v.to_le_bytes());
        }
        let keep = 12u32;
        let out = round_f64_bytes(&src, keep);
        assert_eq!(out.len(), src.len());
        let mut buf = [0u8; 8];
        for (i, chunk) in out.chunks_exact(8).enumerate() {
            buf.copy_from_slice(chunk);
            let got = f64::from_le_bytes(buf);
            let want = round_f64(values[i], keep);
            // NaN compares unequal; check bit patterns for the NaN entry.
            assert_eq!(got.to_bits(), want.to_bits(), "value {i}");
        }
    }

    #[test]
    fn round_f32_bytes_matches_scalar() {
        let values: Vec<f32> = vec![1.0, -3.5, 1234.5, 9.87e6];
        let mut src = Vec::new();
        for v in &values {
            src.extend_from_slice(&v.to_le_bytes());
        }
        let out = round_f32_bytes(&src, 10);
        let mut buf = [0u8; 4];
        for (i, chunk) in out.chunks_exact(4).enumerate() {
            buf.copy_from_slice(chunk);
            assert_eq!(
                f32::from_le_bytes(buf),
                round_f32(values[i], 10),
                "value {i}"
            );
        }
    }

    #[test]
    fn round_f32_preserves_non_finite_and_zero() {
        assert!(round_f32(f32::NAN, 5).is_nan());
        assert_eq!(round_f32(f32::INFINITY, 5), f32::INFINITY);
        assert_eq!(round_f32(0.0, 5).to_bits(), 0.0f32.to_bits());
        assert_eq!(round_f32(-0.0, 5).to_bits(), (-0.0f32).to_bits());
        assert_eq!(round_f32(1.5, 23), 1.5);
    }
}
