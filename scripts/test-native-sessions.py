#!/usr/bin/env python3
"""Generic native SESSION equality, bridge, and checkpoint SQL oracle.

The bounded SQL fixture has no application schema or policy. It runs with the
existing external_sql_checkpoint_capture harness, which recreates the worker.
The default one-minute watermark lag admits out-of-order bridge rows. The
--late-input variant uses the direct event-time watermark with one-row source
batches, so each row can advance the watermark before the next arrives.
"""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

BASE = datetime.fromisoformat("2023-10-09T17:13:20")
EVENTS = [("a", 0, 1), ("b", 1, 5), ("a", 12, 2), ("a", 10, 3),
          ("b", 11, 4), ("a", 40, 7), ("a", 23, 6), ("a", 33, 8)]
FIELDS = {"segment", "start", "end", "n", "total", "lo", "hi"}


def iso(offset):
    return (BASE + timedelta(seconds=offset)).isoformat()


def expected(late_input=False, nested_groups=False):
    if nested_groups:
        return canonical(expected(late_input) + [
            dict(segment="marker", start=iso(120), end=iso(131), n=2, total=24, lo=11, hi=13),
            dict(segment="marker", start=iso(240), end=iso(250), n=1, total=17, lo=17, hi=17),
        ])
    if late_input:
        return canonical([
            dict(segment="a", start=iso(0), end=iso(10), n=1, total=1, lo=1, hi=1),
            dict(segment="b", start=iso(1), end=iso(11), n=1, total=5, lo=5, hi=5),
            dict(segment="a", start=iso(12), end=iso(22), n=1, total=2, lo=2, hi=2),
            dict(segment="a", start=iso(40), end=iso(50), n=1, total=7, lo=7, hi=7),
        ])
    return canonical([
        dict(segment="a", start=iso(0), end=iso(22), n=3, total=6, lo=1, hi=3),
        dict(segment="b", start=iso(1), end=iso(21), n=2, total=9, lo=4, hi=5),
        dict(segment="a", start=iso(23), end=iso(50), n=3, total=21, lo=6, hi=8),
    ])


def canonical(rows):
    return sorted(rows, key=lambda row: (row["start"], row["segment"], row["end"]))


def fixture(directory, late_input=False, nested_groups=False):
    directory.mkdir(parents=True, exist_ok=True)
    events = EVENTS + ([("marker", 120, 11), ("marker", 121, 13), ("marker", 240, 17)]
                       if nested_groups else [])
    (directory / "input.jsonl").write_text("".join(
        json.dumps(dict(timestamp=iso(second), segment=segment, metric=metric)) + "\n"
        for segment, second, metric in events))
    watermark = ("WATERMARK FOR timestamp" if late_input else
                 "WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute'")
    grouping = """SELECT segment, SESSION(INTERVAL '10 seconds') AS window,
         COUNT(*) AS n, SUM(metric) AS total, MIN(metric) AS lo, MAX(metric) AS hi
  FROM session_input GROUP BY segment, window"""
    if nested_groups:
        # Each GROUP BY over completed window data uses the existing exact-bin
        # operator; retain the complete caller-defined window and group key.
        for _ in range(2):
            grouping = f"""SELECT segment, window, SUM(n) AS n, SUM(total) AS total,
         MIN(lo) AS lo, MAX(hi) AS hi FROM ({grouping}) GROUP BY segment, window"""
    (directory / "query.sql").write_text(f"""
CREATE TABLE session_input (timestamp TIMESTAMP NOT NULL, segment TEXT, metric BIGINT,
  {watermark})
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE session_output (segment TEXT, start TIMESTAMP, end TIMESTAMP,
  n BIGINT, total BIGINT, lo BIGINT, hi BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'json', type = 'sink');
INSERT INTO session_output
SELECT segment, window.start, window.end, n, total, lo, hi FROM (
  {grouping}
);
""")
    (directory / "expected.json").write_text(json.dumps(expected(late_input, nested_groups), indent=2) + "\n")


def output(path):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert all(set(row) == FIELDS for row in rows), (path, rows)
    assert all(type(row[field]) is int for row in rows for field in ("n", "total", "lo", "hi")), (path, rows)
    assert all(type(row[field]) is str for row in rows for field in ("segment", "start", "end")), (path, rows)
    assert len(rows) == len({(row["segment"], row["start"], row["end"]) for row in rows}), (path, rows)
    return canonical(rows)


def run(binary, directory, backend, batch, protocol, late_input=False, nested_groups=False):
    fixture(directory, late_input, nested_groups)
    expected_rows = expected(late_input, nested_groups)
    env = dict(os.environ)
    for flag in ("STREAMR_TEST_TYPED_SQL", "STREAMR_TEST_NATIVE_AGGREGATES"):
        env.pop(flag, None)
    env.update(STREAMR_TEST_NATIVE_WINDOWS="1", STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol, STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="1",
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(len(expected_rows)),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="0",
        STREAMR_CAPTURE_EXPECTED_ROWS=str(len(expected_rows)), STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
    if nested_groups:
        # Three native owners need 64KiB checkpoint pages to cover the 24KiB
        # SQL row bound plus encoding; the two-owner default scan pool is too small.
        env.update(STREAMR_TEST_MAX_OPEN_DATABASES="3", STREAMR_TEST_MAX_SNAPSHOTS="6",
                   STREAMR_TEST_SCAN_PAGE_BYTES="3145728",
                   # Each owner reserves 5x its 512KiB write scope plus containers.
                   STREAMR_TEST_QUEUED_WRITE_BYTES="12582912",
                   STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="9",
                   STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(len(expected(late_input))))
    with (directory / "capture.log").open("w") as log:
        result = subprocess.run([str(binary), "external_sql_checkpoint_capture", "--ignored",
            "--test-threads=1", "--nocapture"], env=env, stdout=log,
            stderr=subprocess.STDOUT, timeout=180)
    text = (directory / "capture.log").read_text()
    if result.returncode or "1 passed" not in text:
        raise RuntimeError(f"native SESSION capture failed: {directory / 'capture.log'}")
    for path in (directory / "output.initial.jsonl", directory / "output.jsonl"):
        assert output(path) == expected_rows, (path, output(path), expected_rows)
    prefix = expected(late_input) if nested_groups else []
    assert output(directory / "output.checkpoint-1.jsonl") == prefix
    print(f"PASS {directory.name}: {len(expected_rows)} exact sessions, committed prefix {len(prefix)}, full checkpoint recovery", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--late-input", action="store_true",
                        help="Use direct event-time watermark and assert late rows drop")
    parser.add_argument("--nested-groups", action="store_true",
                        help="Add two exact-timestamp GROUP BY stages over completed sessions")
    args = parser.parse_args()
    root = args.directory.resolve()
    if not args.binary:
        fixture(root, args.late_input, args.nested_groups)
        print(root / "query.sql")
        return
    binary = args.binary.resolve(strict=True)
    for backend in ("memory", "rocksdb"):
        for batch in ((1,) if args.late_input else (1, 8)):
            for protocol in ("controller", "leader"):
                run(binary, root / f"{backend}-{batch}-{protocol}", backend, batch, protocol,
                    args.late_input, args.nested_groups)


if __name__ == "__main__":
    main()
