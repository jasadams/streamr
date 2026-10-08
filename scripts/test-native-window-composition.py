#!/usr/bin/env python3
"""Qualify ordinary SQL reaggregation of closed HOP rows.

Prepare fixtures without --binary. With a fresh arroyo-sql-testing test binary,
run the generic checkpoint-capture harness across both configured backends,
both checkpoint protocols, and source batch targets 1 and 8. This proves a
finite reaggregation of closed windows; it does not assert a moving count or
quiet-period zero emission.
"""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess


BASE = datetime.fromisoformat("2023-10-09T17:13:20")
OFFSETS = (1, 3, 7)
FIELDS = {"k", "closed_windows", "pane_memberships", "peak", "latest_end"}


def expected() -> dict:
    """Compute HOP memberships independently of the SQL operators."""
    windows = {}
    for second in OFFSETS:
        pane = (second // 2) * 2
        for start in (pane - 2, pane):
            assert start <= second < start + 4
            windows[start] = windows.get(start, 0) + 1
    latest = BASE + timedelta(seconds=max(windows) + 4)
    # Debezium JSON serializes SQL TIMESTAMP as UTC epoch milliseconds; the
    # append-only JSON window sink uses an ISO string instead.
    latest_epoch_ms = int((latest - datetime(1970, 1, 1)).total_seconds() * 1000)
    result = {
        "k": "a",
        "closed_windows": len(windows),
        "pane_memberships": sum(windows.values()),
        "peak": max(windows.values()),
        "latest_end": latest_epoch_ms,
    }
    assert result == {
        "k": "a", "closed_windows": 5, "pane_memberships": 6,
        "peak": 2, "latest_end": 1696871610000,
    }
    return result


def write_fixture(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "input.jsonl").open("w") as output:
        for second in OFFSETS:
            output.write(json.dumps({
                "timestamp": (BASE + timedelta(seconds=second)).isoformat(),
                "k": "a",
            }) + "\n")
    (directory / "query.sql").write_text(f"""
SET updating_ttl = NULL;
CREATE TABLE composition_input (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL,
  WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute')
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE composition_output (k TEXT, closed_windows BIGINT,
  pane_memberships BIGINT, peak BIGINT, latest_end TIMESTAMP)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW closed_hops AS
  SELECT k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
         COUNT(*) AS n
  FROM composition_input GROUP BY k, window;
CREATE VIEW finalized_hops AS
  SELECT k, window.end AS window_end, n FROM closed_hops;
INSERT INTO composition_output
SELECT k, COUNT(*) AS closed_windows, SUM(n) AS pane_memberships,
       MAX(n) AS peak, MAX(window_end) AS latest_end
FROM finalized_hops GROUP BY k;
""")
    (directory / "expected.final.json").write_text(
        json.dumps({"a": expected()}, indent=2) + "\n"
    )
    (directory / "expected.checkpoint.json").write_text("{}\n")


def strict_reduce(path: Path, wanted: dict, record_count: int) -> None:
    """Check full Debezium values and every before-image transition."""
    current = {}
    count = 0
    with path.open() as source:
        for line in source:
            count += 1
            record = json.loads(line)
            assert isinstance(record, dict), (path, count, record)
            row = record.get("payload", record)
            assert isinstance(row, dict), (path, count, row)
            assert {"before", "after", "op"} <= row.keys(), (path, count, row)
            before, after, op = row["before"], row["after"], row["op"]
            assert op in {"c", "u", "d"}, (path, count, row)
            assert (before is None) == (op == "c"), (path, count, row)
            assert (after is None) == (op == "d"), (path, count, row)
            value = after if after is not None else before
            assert isinstance(value, dict) and set(value) == FIELDS, (path, count, value)
            key = value["k"]
            assert current.get(key) == before, (path, count, current, row)
            if after is None:
                del current[key]
            else:
                assert set(after) == FIELDS, (path, count, after)
                current[key] = after
    assert count == record_count, (path, count, record_count)
    assert current == wanted, (path, current, wanted)


def run_case(binary: Path, directory: Path, backend: str, batch: int, mode: str) -> None:
    write_fixture(directory)
    for name in ("output.jsonl", "output.initial.jsonl", "capture.log"):
        (directory / name).unlink(missing_ok=True)
    environment = dict(os.environ)
    environment.pop("STREAMR_TEST_TYPED_SQL", None)
    environment.update(
        STREAMR_TEST_NATIVE_WINDOWS="1",
        STREAMR_TEST_NATIVE_AGGREGATES="1",
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=mode,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="1",
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="0",
        STREAMR_CAPTURE_EXPECTED_ROWS="1",
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    log = directory / "capture.log"
    with log.open("w") as output:
        completed = subprocess.run(
            [str(binary), "external_sql_checkpoint_capture", "--ignored",
             "--test-threads=1", "--nocapture"],
            env=environment, stdout=output, stderr=subprocess.STDOUT,
        )
    if completed.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"capture failed: {log}")
    wanted = {"a": expected()}
    strict_reduce(directory / "output.initial.jsonl", wanted, 1)
    strict_reduce(directory / "output.jsonl", wanted, 1)
    # The capture harness also asserts that the checkpoint prefix has no sink
    # row. Its delayed event-time watermark closes these windows at EOF.
    print(f"PASS {directory.name}: exact closed-window reaggregation and recovery", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path, help="fresh arroyo-sql-testing test binary")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.binary is None:
        write_fixture(directory)
        print(f"Prepared {directory}; final={expected()}, checkpoint={{}}")
        return
    binary = args.binary.resolve(strict=True)
    for backend in ("memory", "rocksdb"):
        for batch in (1, 8):
            for mode in ("controller", "leader"):
                run_case(binary, directory / f"{backend}-{batch}-{mode}", backend, batch, mode)


if __name__ == "__main__":
    main()
