#!/usr/bin/env python3
"""Qualify legacy SQL SESSION exact-gap equality across batching and worker replay."""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

BASE = datetime.fromisoformat("2023-10-09T17:13:20")
EXPECTED = [{"segment": "a", "start": BASE.isoformat(),
             "end": (BASE + timedelta(seconds=22)).isoformat(), "n": 3}]


def fixture(directory):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text("".join(
        json.dumps({"timestamp": (BASE + timedelta(seconds=second)).isoformat(),
                    "segment": "a", "metric": 1}) + "\n"
        for second in (0, 10, 12)))
    (directory / "query.sql").write_text(f"""
CREATE TABLE session_input (timestamp TIMESTAMP, segment TEXT, metric BIGINT,
  WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE session_output (segment TEXT, start TIMESTAMP, end TIMESTAMP, n BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'json', type = 'sink');
INSERT INTO session_output
SELECT segment, window.start, window.end, n FROM (
  SELECT segment, SESSION(INTERVAL '10 seconds') AS window, COUNT(*) AS n
  FROM session_input GROUP BY segment, window
);
""")
    (directory / "expected.json").write_text(json.dumps(EXPECTED, indent=2) + "\n")


def exact_rows(path):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows == EXPECTED, (path, rows, EXPECTED)


def run(binary, directory, batch, protocol):
    fixture(directory)
    env = dict(os.environ)
    for flag in ("STREAMR_TEST_NATIVE_WINDOWS", "STREAMR_TEST_NATIVE_AGGREGATES", "STREAMR_TEST_TYPED_SQL"):
        env.pop(flag, None)
    env.update(STREAMR_TEST_BACKEND="memory", STREAMR_TEST_CHECKPOINT_MODE=protocol,
               STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
               STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
               STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
               STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="1",
               STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
               STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="0",
               STREAMR_CAPTURE_EXPECTED_ROWS="1", STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
    with (directory / "capture.log").open("w") as log:
        result = subprocess.run([str(binary), "external_sql_checkpoint_capture", "--ignored",
                                 "--test-threads=1", "--nocapture"], env=env, stdout=log,
                                stderr=subprocess.STDOUT, timeout=180)
    if result.returncode or "1 passed" not in (directory / "capture.log").read_text():
        raise RuntimeError(f"legacy SESSION capture failed: {directory / 'capture.log'}")
    exact_rows(directory / "output.initial.jsonl")
    exact_rows(directory / "output.jsonl")
    print(f"PASS legacy SESSION batch={batch} protocol={protocol}: [0,22) n=3 before/after replay", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    root = args.directory.resolve()
    if not args.binary:
        fixture(root)
        print(root / "query.sql")
        return
    binary = args.binary.resolve(strict=True)
    for batch in (1, 128):
        for protocol in ("controller", "leader"):
            run(binary, root / f"batch-{batch}-{protocol}", batch, protocol)


if __name__ == "__main__":
    main()
