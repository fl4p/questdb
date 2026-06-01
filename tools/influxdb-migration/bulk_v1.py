#!/usr/bin/env python3
"""Direct TSM bulk import for an InfluxDB v1 source.

This is the FAST, complete-import data path for v1, the analog of the v2
``influxd inspect export-lp`` route. It bypasses the InfluxQL query engine
entirely -- which does not scale to very high series cardinality (server-side
pivots time out on buckets with tens of thousands of series) -- and instead
shells out to ``influx_inspect export``, which reads the shard TSM/WAL files
directly and streams InfluxDB line protocol. We rewrite each line's measurement
token to honour the tool's ``<db>_<measurement>`` naming contract (see
``model.table_name`` / ``model.scope_prefix`` and ``README.md``) and feed the
result to QuestDB's ILP-over-HTTP ``/write`` endpoint in batches.

Only the bulk DATA movement lives here. ACL/principals/manifest generation stays
with ``influx_migrate.py`` + ``acl_from_artifacts.py``; this module deliberately
does not touch them.

The pure helpers (``rewrite_measurement``, ``rewrite_line``, ``build_export_cmd``,
``parse_basic_or_token_auth``) carry no I/O so the test suite can exercise the
tricky line-protocol escaping without a real ``influx_inspect`` or QuestDB.

Examples
--------
  # dry run: report target tables + line counts, POST nothing
  bulk_v1.py --database mydb --datadir /var/lib/influxdb/data \\
      --waldir /var/lib/influxdb/wal --questdb-url http://localhost:9000 \\
      --dry-run

  # real run, scoped to one retention policy and time range, with basic auth
  bulk_v1.py --database mydb --retention autogen \\
      --datadir /var/lib/influxdb/data --waldir /var/lib/influxdb/wal \\
      --start 2023-01-01T00:00:00Z --end 2023-02-01T00:00:00Z \\
      --questdb-url http://localhost:9000 --user admin --password secret
"""

from __future__ import annotations

import argparse
import base64
import logging
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Dict, Iterable, Iterator, List, Optional

from model import scope_prefix, validate_scope

log = logging.getLogger("influx_migrate.bulk_v1")

# influx_inspect writes header comments (starting with '#') even with -lponly in
# some builds (e.g. the "# writing wal/tsm data" progress markers), and the
# pure line-protocol body never starts with '#'. We skip comment and blank lines
# defensively so the rewrite only ever sees real points.
_COMMENT_PREFIX = "#"


def build_export_cmd(
    database: str,
    datadir: str,
    waldir: str,
    retention: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    measurement: Optional[str] = None,
    compress: bool = False,
    binary: str = "influx_inspect",
) -> List[str]:
    """Build the ``influx_inspect export`` argv that streams line protocol.

    ``-lponly`` makes the export emit pure line protocol with no DDL/CREATE
    DATABASE header, which is exactly what we re-feed to QuestDB. ``-out -``
    streams to stdout so we can pipe it line by line instead of staging a file.

    ``-database``/``-retention`` scope the export; ``-start``/``-end`` (RFC3339)
    bound the time range; ``-datadir``/``-waldir`` point at the v1 data and WAL
    directories. ``-compress`` gzips the output -- only pass it when the caller
    is prepared to gunzip the stream (the streaming reader here expects text, so
    the CLI leaves it off by default).

    NOTE: ``influx_inspect export`` does not expose a measurement filter flag, so
    a requested ``measurement`` is honoured downstream by filtering the streamed
    lines (see :func:`stream_export_lines`) rather than at the source.
    """
    cmd = [
        binary,
        "export",
        "-lponly",
        "-database",
        database,
        "-datadir",
        datadir,
        "-waldir",
        waldir,
        "-out",
        "-",
    ]
    if retention:
        cmd += ["-retention", retention]
    if start:
        cmd += ["-start", start]
    if end:
        cmd += ["-end", end]
    if compress:
        cmd += ["-compress"]
    return cmd


def _measurement_end(line: str) -> int:
    """Return the index of the first UNESCAPED comma or space in ``line``.

    In line protocol the measurement runs from the start of the line up to the
    first unescaped ``,`` (which begins the tag set) or unescaped `` `` (which
    begins the field set when there are no tags). Inside the measurement a
    literal comma or space is escaped with a backslash, and the backslash itself
    is not special otherwise, so we only treat a ``,``/`` `` as a delimiter when
    it is not immediately preceded by an odd run of backslashes. Walking the
    string once and tracking whether the previous char was an escaping backslash
    keeps the parse O(n) and correct for runs like ``\\\\,`` (escaped backslash
    followed by a real delimiter).
    """
    escaped = False
    for i, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "," or ch == " ":
            return i
    return len(line)


def _unescape_measurement(raw: str) -> str:
    """Decode a line-protocol-escaped measurement token to its literal name.

    Only ``,`` and `` `` are escaped in a measurement; a backslash escapes the
    next char. We collapse each ``\\x`` to ``x`` so the result is the name the
    naming contract (and ``validate_scope`` on the db) reasons about.
    """
    out: List[str] = []
    escaped = False
    for ch in raw:
        if escaped:
            out.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        else:
            out.append(ch)
    if escaped:  # trailing lone backslash: keep it verbatim
        out.append("\\")
    return "".join(out)


def _escape_measurement(name: str) -> str:
    """Re-encode a literal measurement name for line protocol.

    The prefix we prepend (``<db>_``) is a clean ``[A-Za-z0-9_]`` identifier, so
    it never needs escaping; but the original measurement may contain commas or
    spaces, so we re-escape the whole assembled name to stay valid LP.
    """
    out: List[str] = []
    for ch in name:
        if ch == "," or ch == " " or ch == "\\":
            out.append("\\")
        out.append(ch)
    return "".join(out)


def rewrite_measurement(raw_measurement: str, prefix: str) -> str:
    """Prefix one (escaped) measurement token, preserving LP escaping.

    ``raw_measurement`` is the token exactly as it appears in the line (still
    escaped). We unescape it to the literal name, prepend ``prefix`` (already a
    clean identifier ending in ``_``), then re-escape the assembled name so any
    comma/space in the ORIGINAL measurement survives. The prefix contributes no
    escapable characters, so prefixing the unescaped name and re-escaping is
    equivalent to ``prefix + raw_measurement`` for clean prefixes -- but going
    through the literal form keeps the contract identical to ``table_name`` and
    is robust if the prefix policy ever changes.
    """
    literal = _unescape_measurement(raw_measurement)
    return _escape_measurement(prefix + literal)


def rewrite_line(line: str, prefix: str) -> str:
    """Rewrite a full line-protocol line's measurement to ``prefix + name``.

    The remainder of the line (tag set, field set, optional timestamp) is left
    byte-for-byte untouched: only the measurement token, which is everything up
    to the first unescaped ``,`` or `` ``, changes. Blank lines and comment
    lines are returned unchanged (the caller filters them out first; this is a
    belt-and-braces guard).
    """
    if not line or line.startswith(_COMMENT_PREFIX):
        return line
    cut = _measurement_end(line)
    raw_measurement = line[:cut]
    rest = line[cut:]
    return rewrite_measurement(raw_measurement, prefix) + rest


def measurement_of(line: str) -> str:
    """Return the literal (unescaped) measurement name of a raw LP line."""
    return _unescape_measurement(line[: _measurement_end(line)])


def stream_export_lines(
    cmd: List[str],
    runner: Optional["LineRunner"] = None,
) -> Iterator[str]:
    """Run the export command and yield its stdout, one stripped line at a time.

    ``runner`` is the test/subprocess seam: a callable that takes the argv and
    returns an iterable of lines. The default :class:`_SubprocessRunner` actually
    spawns ``influx_inspect``; tests inject a list-backed fake so no real binary
    is needed. Comment and blank lines are dropped here so downstream code only
    ever sees real points.
    """
    run = runner or _SubprocessRunner()
    for raw in run(cmd):
        line = raw.rstrip("\n")
        if not line or line.startswith(_COMMENT_PREFIX):
            continue
        yield line


class LineRunner:
    """Callable seam: argv -> iterable of stdout lines. See _SubprocessRunner."""

    def __call__(self, cmd: List[str]) -> Iterable[str]:  # pragma: no cover
        raise NotImplementedError


class _SubprocessRunner(LineRunner):
    """Default runner: spawn the export process and stream its stdout."""

    def __call__(self, cmd: List[str]) -> Iterable[str]:
        log.info("running: %s", " ".join(cmd))
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True,
            )
        except FileNotFoundError as exc:
            raise SystemExit(
                "could not run %r: is influx_inspect on PATH (or use --binary)?"
                % cmd[0]
            ) from exc
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                yield line
        finally:
            proc.stdout.close()
            ret = proc.wait()
            err = proc.stderr.read() if proc.stderr else ""
            if proc.stderr:
                proc.stderr.close()
            if ret != 0:
                raise SystemExit(
                    "influx_inspect export failed (exit %d): %s" % (ret, err.strip())
                )


def parse_basic_or_token_auth(
    user: Optional[str], password: Optional[str], token: Optional[str]
) -> Optional[str]:
    """Build an HTTP Authorization header value from explicit auth flags.

    A bearer ``token`` wins if present; otherwise a ``user`` (with optional
    ``password``) yields HTTP basic. Returns None when no auth was supplied, so
    an unauthenticated QuestDB just gets no header. Mirrors the auth derivation
    in ``influx_migrate.py`` but driven by explicit flags rather than an ILP
    conf string, since this path talks raw HTTP to ``/write``.
    """
    if token:
        return "Bearer " + token
    if user:
        raw = ("%s:%s" % (user, password or "")).encode("utf-8")
        return "Basic " + base64.b64encode(raw).decode("ascii")
    return None


class _IlpHttpFeeder:
    """Buffers rewritten LP lines and POSTs them to ``/write`` in batches.

    QuestDB's ILP-over-HTTP endpoint accepts a newline-delimited LP body at
    ``/write`` (and the ``/api/v2/write`` InfluxDB-compat alias). We accumulate
    ``batch_size`` lines, join them with ``\\n``, and POST once per batch so the
    cost of the HTTP round-trip is amortised. Only stdlib ``urllib`` is used.
    """

    def __init__(
        self,
        base_url: str,
        batch_size: int,
        auth: Optional[str],
        timeout: float = 60.0,
    ):
        self._url = base_url.rstrip("/") + "/write"
        self._batch_size = max(1, batch_size)
        self._auth = auth
        self._timeout = timeout
        self._buf: List[str] = []
        self.lines_sent = 0
        self.batches_sent = 0

    def add(self, line: str) -> None:
        self._buf.append(line)
        if len(self._buf) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        body = ("\n".join(self._buf) + "\n").encode("utf-8")
        req = urllib.request.Request(self._url, data=body, method="POST")
        req.add_header("Content-Type", "text/plain; charset=utf-8")
        if self._auth:
            req.add_header("Authorization", self._auth)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace").strip()
            raise SystemExit(
                "QuestDB /write rejected a batch (HTTP %d): %s"
                % (exc.code, detail)
            ) from exc
        except urllib.error.URLError as exc:
            raise SystemExit("QuestDB /write unreachable: %s" % exc.reason) from exc
        self.lines_sent += len(self._buf)
        self.batches_sent += 1
        self._buf.clear()


def run_bulk_import(
    database: str,
    cmd: List[str],
    prefix: str,
    feeder: Optional["_IlpHttpFeeder"],
    measurement: Optional[str] = None,
    runner: Optional[LineRunner] = None,
) -> Dict[str, int]:
    """Stream the export, rewrite each line, feed QuestDB (or count for dry-run).

    Returns a per-target-table line count. When ``feeder`` is None this is a dry
    run: lines are rewritten and counted but nothing is POSTed. An optional
    ``measurement`` filter keeps only lines whose source measurement matches
    (``influx_inspect`` has no measurement flag, so we filter the stream).
    """
    counts: Dict[str, int] = {}
    for line in stream_export_lines(cmd, runner=runner):
        src = measurement_of(line)
        if measurement is not None and src != measurement:
            continue
        rewritten = rewrite_line(line, prefix)
        target = prefix + src
        counts[target] = counts.get(target, 0) + 1
        if feeder is not None:
            feeder.add(rewritten)
    if feeder is not None:
        feeder.flush()
    return counts


def _plan_prefix(database: str, use_prefix: bool) -> str:
    """Return the measurement prefix for this run, validating the db name.

    With a prefix the db must be a clean identifier (the table prefix is a
    literal ``<db>_``); ``--no-prefix`` yields an empty prefix for single-db
    setups, exactly like the main tool's ``--no-prefix``.
    """
    if not use_prefix:
        return ""
    validate_scope(database)
    return scope_prefix(database)


def _report(counts: Dict[str, int], dry_run: bool) -> None:
    verb = "[dry-run] would import" if dry_run else "imported"
    total = 0
    for table, n in sorted(counts.items()):
        total += n
        log.info("%s %d lines -> %s", verb, n, table)
    if not counts:
        log.info("%s 0 lines (export produced no points)", verb)
    log.info("%s %d lines across %d table(s)", verb, total, len(counts))


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        prefix = _plan_prefix(args.database, not args.no_prefix)
    except Exception as exc:  # InvalidScopeName and friends
        log.error(
            "database %r is not a clean [A-Za-z0-9_] identifier; rename it "
            "upstream (the QuestDB table prefix is a literal '<db>_'). Aborting.",
            args.database,
        )
        return 2

    cmd = build_export_cmd(
        database=args.database,
        datadir=args.datadir,
        waldir=args.waldir,
        retention=args.retention,
        start=args.start,
        end=args.end,
        measurement=args.measurement,
        compress=False,
        binary=args.binary,
    )

    feeder: Optional[_IlpHttpFeeder] = None
    if not args.dry_run:
        auth = parse_basic_or_token_auth(args.user, args.password, args.token)
        feeder = _IlpHttpFeeder(args.questdb_url, args.batch_size, auth)

    counts = run_bulk_import(
        database=args.database,
        cmd=cmd,
        prefix=prefix,
        feeder=feeder,
        measurement=args.measurement,
    )
    _report(counts, args.dry_run)
    if feeder is not None:
        log.info(
            "POSTed %d lines in %d batch(es) to %s/write",
            feeder.lines_sent,
            feeder.batches_sent,
            args.questdb_url.rstrip("/"),
        )
    return 0


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="bulk_v1.py",
        description="Direct TSM bulk import of an InfluxDB v1 database into "
        "QuestDB via influx_inspect export + ILP-over-HTTP.",
    )
    src = p.add_argument_group("source (InfluxDB v1, via influx_inspect)")
    src.add_argument("--database", required=True, help="v1 database name")
    src.add_argument("--retention", help="optional retention policy to scope the export")
    src.add_argument("--datadir", required=True, help="v1 data dir (TSM shards)")
    src.add_argument("--waldir", required=True, help="v1 WAL dir")
    src.add_argument("--start", help="RFC3339 inclusive lower time bound")
    src.add_argument("--end", help="RFC3339 exclusive upper time bound")
    src.add_argument(
        "--measurement",
        help="optional single-measurement filter (applied to the streamed lines; "
        "influx_inspect export has no measurement flag)",
    )
    src.add_argument(
        "--binary",
        default="influx_inspect",
        help="path to the influx_inspect binary (default: looked up on PATH)",
    )

    tgt = p.add_argument_group("target (QuestDB ILP-over-HTTP /write)")
    tgt.add_argument(
        "--questdb-url",
        required=True,
        help="QuestDB HTTP base, e.g. http://localhost:9000",
    )
    tgt.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="lines per /write POST (default 10000)",
    )
    tgt.add_argument("--user", help="HTTP basic-auth user")
    tgt.add_argument("--password", help="HTTP basic-auth password")
    tgt.add_argument("--token", help="bearer token (wins over --user/--password)")

    name = p.add_argument_group("naming")
    name.add_argument(
        "--no-prefix",
        action="store_true",
        help="single-DB setups: keep bare measurement names (no <db>_ prefix)",
    )

    ops = p.add_argument_group("ops")
    ops.add_argument(
        "--dry-run",
        action="store_true",
        help="rewrite + count lines per target table, POST nothing",
    )
    ops.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
