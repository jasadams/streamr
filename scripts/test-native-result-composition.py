#!/usr/bin/env python3
"""Qualify existing-SQL lifetime and latest closed-HOP composition.

The query uses only native HOP, updating aggregates, UNION ALL, and ordinary
aggregate expressions. It checks values and recovery, not autonomous idle
zeros or a general CDC join. Prepare fixtures without --binary; with a fresh
SQL test binary, run memory/RocksDB, controller/leader, source batch 1/8.
"""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess


BASE = datetime.fromisoformat("2023-10-09T17:13:20")
EVENTS = ((1, 1), (3, 2), (7, 3))
FIELDS = {"k", "lifetime_count", "recent_count"}
CHECKPOINT = {"k": "x", "lifetime_count": 1, "recent_count": None}
FINAL = {"k": "x", "lifetime_count": 3, "recent_count": 1}


def window_counts() -> dict[int, int]:
    windows = {}
    for second, _ in EVENTS:
        pane = second // 2 * 2
        for start in (pane - 2, pane):
            assert start <= second < start + 4
            windows[start] = windows.get(start, 0) + 1
    return windows


def independent_oracle() -> None:
    windows = window_counts()
    assert sum(windows.values()) == 6
    assert max(windows) + 4 == 10
    assert (len(EVENTS), windows[max(windows)]) == (3, FINAL["recent_count"])


def write_fixture(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text("".join(
        json.dumps({
            "timestamp": (BASE + timedelta(seconds=second)).isoformat(),
            "k": "x", "v": value,
        }) + "\n" for second, value in EVENTS
    ))
    (directory / "query.sql").write_text(f"""
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute')
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE composed_out (k TEXT, lifetime_count BIGINT, recent_count BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW lifetime AS SELECT k, COUNT(*) AS lifetime_count
  FROM events GROUP BY k;
CREATE VIEW rolling AS SELECT k,
  HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
  COUNT(*) AS recent_count FROM events GROUP BY k, window;
CREATE VIEW closed AS SELECT k, window.end AS window_end,
  recent_count FROM rolling;
CREATE VIEW latest_rolling AS SELECT k,
  LAST_VALUE(recent_count ORDER BY window_end) AS recent_count
  FROM closed GROUP BY k;
CREATE VIEW normalized AS
  SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime
  UNION ALL
  SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling;
INSERT INTO composed_out SELECT k, MAX(lifetime_count) AS lifetime_count,
  MAX(recent_count) AS recent_count FROM normalized GROUP BY k;
""")
    (directory / "expected.checkpoint.json").write_text(
        json.dumps(CHECKPOINT, indent=2) + "\n"
    )
    (directory / "expected.final.json").write_text(json.dumps(FINAL, indent=2) + "\n")


def strict_reduce(path: Path, minimum_rows: int, maximum_rows: int,
                  final: dict, first: dict | None = None) -> None:
    current = None
    count = 0
    with path.open() as source:
        for line in source:
            count += 1
            record = json.loads(line)
            assert isinstance(record, dict), (path, count, record)
            row = record.get("payload", record)
            assert isinstance(row, dict) and {"before", "after", "op"} <= row.keys(), row
            before, after, op = row["before"], row["after"], row["op"]
            assert op in {"c", "u", "d"}, (path, count, row)
            assert (before is None) == (op == "c"), (path, count, row)
            assert op != "d" and after is not None, (path, count, row)
            assert before == current, (path, count, before, current)
            if op == "u":
                assert before != after, (path, count, row)
            assert isinstance(after, dict) and set(after) == FIELDS, (path, count, after)
            assert after["k"] == "x"
            lifetime = after["lifetime_count"]
            recent = after["recent_count"]
            assert lifetime is not None or recent is not None, (path, count, after)
            assert lifetime is None or (
                type(lifetime) is int and 1 <= lifetime <= FINAL["lifetime_count"]
            ), (path, count, after)
            # An early aggregate tick can expose a prior closed HOP count
            # before the final winner. Both counts derive from actual windows.
            assert recent is None or (
                type(recent) is int and recent in window_counts().values()
            ), (path, count, after)
            if current is not None:
                if current["lifetime_count"] is not None:
                    assert lifetime is not None and lifetime >= current["lifetime_count"], (
                        path, count, after
                    )
                if current["recent_count"] is not None:
                    assert recent is not None, (path, count, after)
            if count == 1 and first is not None:
                assert after == first, (path, after, first)
            current = after
    assert minimum_rows <= count <= maximum_rows, (path, count, minimum_rows, maximum_rows)
    assert current == final, (path, current, final)


def run_case(binary: Path, directory: Path, backend: str, batch: int, mode: str) -> None:
    write_fixture(directory)
    for name in ("output.jsonl", "output.initial.jsonl", "capture.log"):
        (directory / name).unlink(missing_ok=True)
    env = dict(os.environ)
    env.pop("STREAMR_TEST_TYPED_SQL", None)
    env.update(
        STREAMR_TEST_NATIVE_AGGREGATES="1",
        STREAMR_TEST_NATIVE_WINDOWS="1",
        STREAMR_TEST_MAX_OPEN_DATABASES="4",
        STREAMR_TEST_MAX_SNAPSHOTS="4",
        STREAMR_TEST_SCAN_PAGE_BYTES="4194304",
        # Three 2 MiB aggregate write scopes plus one 512 KiB window scope
        # reserve over 32 MiB together: AdmittedWriteBatch charges 5x bytes
        # plus per-operation metadata before any write.
        STREAMR_TEST_QUEUED_WRITE_BYTES="67108864",
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=mode,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="1",
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
        # Each upstream aggregate can flush once at its initial tick and once
        # at EOF. Interleaving during terminal window drain can expose a
        # closed-window count of 2 before the final latest count of 1.
        STREAMR_CAPTURE_MAX_INITIAL_ROWS="4",
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="1",
        STREAMR_CAPTURE_EXPECTED_ROWS="2",
        STREAMR_CAPTURE_MAX_ROWS="5",
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    log = directory / "capture.log"
    with log.open("w") as output:
        result = subprocess.run(
            [str(binary), "external_sql_checkpoint_capture", "--ignored",
             "--test-threads=1", "--nocapture"],
            env=env, stdout=output, stderr=subprocess.STDOUT,
        )
    if result.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"capture failed: {log}")
    strict_reduce(directory / "output.initial.jsonl", 1, 4, FINAL)
    strict_reduce(directory / "output.jsonl", 2, 5, FINAL, first=CHECKPOINT)
    print(f"PASS {directory.name}: exact lifetime/latest closed HOP CDC and recovery", flush=True)


def main() -> None:
    independent_oracle()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path, help="fresh arroyo-sql-testing test binary")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.binary is None:
        write_fixture(directory)
        print(f"Prepared {directory}: checkpoint={CHECKPOINT}, final={FINAL}")
        return
    binary = args.binary.resolve(strict=True)
    for backend in ("memory", "rocksdb"):
        for batch in (1, 8):
            for mode in ("controller", "leader"):
                run_case(binary, directory / f"{backend}-{batch}-{mode}", backend, batch, mode)


if __name__ == "__main__":
    main()
