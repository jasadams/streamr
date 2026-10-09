#!/usr/bin/env python3
"""Generic native unordered FIRST/LAST checkpoint fixture for a supplied test binary.

This checks the pinned accumulator's physical input order in a singleton
append-only stream. SQL leaves unordered FIRST/LAST result selection arbitrary;
the fixture makes no public source-order guarantee.
"""

import argparse
import json
import os
import re
from pathlib import Path
import subprocess


EVENTS = [
    {"scope_key": "scope", "event_ms": 100, "note": None},
    {"scope_key": "scope", "event_ms": 50, "note": "start"},
    {"scope_key": "scope", "event_ms": 40, "note": ""},
    {"scope_key": "scope", "event_ms": 30, "note": "end"},
    {"scope_key": "scope", "event_ms": 20, "note": None},
]
CHECKPOINT = {"scope_key": "scope", "events": 2, "first_event_ms": 100,
              "last_event_ms": 50, "first_note": None, "last_nonempty_note": "start",
              "max_event_ms": 100}
FINAL = {**CHECKPOINT, "events": 5, "last_event_ms": 20,
         "last_nonempty_note": "end"}
OUTPUT_FIELDS = {"scope_key", "events", "first_event_ms", "last_event_ms",
                 "first_note", "last_nonempty_note", "max_event_ms"}


def write_case(directory):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in EVENTS))
    (directory / "query.sql").write_text(f"""
SET updating_ttl = NULL;
CREATE TABLE unordered_input (scope_key TEXT NOT NULL, event_ms BIGINT NOT NULL, note TEXT)
WITH (connector = 'single_file', path = '{directory}/input.jsonl',
      format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE unordered_output (scope_key TEXT, events BIGINT, first_event_ms BIGINT,
  last_event_ms BIGINT, first_note TEXT, last_nonempty_note TEXT, max_event_ms BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'debezium_json', type = 'sink');
INSERT INTO unordered_output
SELECT scope_key, COUNT(*) AS events,
  FIRST_VALUE(event_ms) AS first_event_ms,
  LAST_VALUE(event_ms) AS last_event_ms,
  FIRST_VALUE(note) AS first_note,
  LAST_VALUE(note) FILTER (WHERE note IS NOT NULL AND note <> '')
    AS last_nonempty_note,
  MAX(event_ms) AS max_event_ms
FROM unordered_input GROUP BY scope_key;
""")


def prefix_value(prefix):
    rows = EVENTS[:prefix]
    notes = [row["note"] for row in rows if row["note"]]
    return dict(scope_key="scope", events=len(rows), first_event_ms=rows[0]["event_ms"],
                last_event_ms=rows[-1]["event_ms"], first_note=rows[0]["note"],
                last_nonempty_note=notes[-1] if notes else None,
                max_event_ms=max(row["event_ms"] for row in rows))


def reduce_cdc(path, prefix_count=None):
    def distinct_object(pairs):
        result = {}
        for key, value in pairs:
            assert key not in result, (path, "duplicate JSON key", key)
            result[key] = value
        return result

    def check_row(row):
        assert isinstance(row, dict) and set(row) == OUTPUT_FIELDS, (path, row)
        assert isinstance(row["scope_key"], str) and row["scope_key"], (path, row)
        for field in ("events", "first_event_ms", "last_event_ms", "max_event_ms"):
            assert type(row[field]) is int, (path, field, row[field])
        assert row["events"] > 0, (path, row)
        assert row["first_note"] is None or isinstance(row["first_note"], str), (path, row)
        assert row["last_nonempty_note"] is None or (
            isinstance(row["last_nonempty_note"], str) and row["last_nonempty_note"]
        ), (path, row)

    current = None
    position = 0
    rows = 0
    for line in path.read_text().splitlines():
        item = json.loads(line, object_pairs_hook=distinct_object)
        assert isinstance(item, dict), (path, rows, item)
        payload = item["payload"] if set(item) == {"payload"} else item
        assert isinstance(payload, dict) and set(payload) == {"before", "after", "op"}, payload
        before, after, op = payload["before"], payload["after"], payload["op"]
        assert op in {"c", "u"}, payload
        if before is not None:
            check_row(before)
        check_row(after)
        assert current == before, (path, rows, current, before)
        assert (before is None) == (op == "c"), payload
        if op == "u":
            assert before != after, (path, rows, "unchanged update", payload)
        candidates = [index for index in range(position + 1, (prefix_count or len(EVENTS)) + 1)
                      if after == prefix_value(index)]
        assert candidates, (path, "not a forward source-prefix value", after)
        position = candidates[0]
        current = after
        rows += 1
    assert rows > 0, (path, "missing aggregate output")
    return rows, current


def run_case(binary, directory, backend, batch, mode):
    write_case(directory)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("STREAMR_TEST_", "STREAMR_CAPTURE_"))}
    env.update(
               STREAMR_TEST_NATIVE_AGGREGATES="1",
               STREAMR_TEST_BACKEND=backend,
               STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
               STREAMR_TEST_CHECKPOINT_MODE=mode,
               STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
               STREAMR_TEST_EXECUTION_BYTES="16777216",
               STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
               STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
               STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="2",
               STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
               STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="1",
               STREAMR_CAPTURE_EXPECTED_ROWS="1",
               STREAMR_CAPTURE_MAX_INITIAL_ROWS=str(len(EVENTS)),
               STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS="2",
               STREAMR_CAPTURE_MAX_ROWS=str(len(EVENTS)),
               STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
    with (directory / "capture.log").open("w") as log:
        result = subprocess.run([str(binary), "external_sql_checkpoint_capture",
                                 "--ignored", "--test-threads=1", "--nocapture"],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
    assert result.returncode == 0, f"{directory}: test exit {result.returncode}"
    assert "1 passed" in (directory / "capture.log").read_text(), directory
    initial_rows, initial = reduce_cdc(directory / "output.initial.jsonl")
    final_rows, final = reduce_cdc(directory / "output.jsonl")
    assert 1 <= initial_rows <= len(EVENTS) and initial == FINAL, (directory, "initial", initial_rows, initial)
    assert 1 <= final_rows <= len(EVENTS) and final == FINAL, (directory, "recovered", final_rows, final)
    lines = (directory / "output.jsonl").read_text().splitlines()
    checkpoint_path = directory / "checkpoint-prefix.jsonl"
    markers = re.findall(
        r'^CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=2 '
        r'committed_rows=(\d+) rows=(\d+) bytes=\d+ path=(.+) job=\S+$',
        (directory / "capture.log").read_text(), re.MULTILINE)
    assert len(markers) == 1, markers
    committed, captured, output_path = markers[0]
    committed = int(committed)
    assert 1 <= committed <= 2 and int(captured) == final_rows, markers
    assert output_path == str(directory / "output.jsonl"), output_path
    checkpoint_path.write_text("\n".join(lines[:committed]) + "\n")
    checkpoint_rows, checkpoint = reduce_cdc(checkpoint_path, prefix_count=2)
    assert checkpoint_rows == committed and checkpoint == CHECKPOINT, (directory, "checkpoint", checkpoint)
    print(f"PASS {backend}/{batch}/{mode}: initial, checkpoint, recovered values", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path,
                        help="existing arroyo-sql-testing test binary; omit to prepare cases")
    args = parser.parse_args()
    root = args.directory.resolve()
    binary = args.binary.resolve(strict=True) if args.binary else None
    for backend in ("memory", "rocksdb"):
        for batch in (1, 8):
            for mode in ("controller", "leader"):
                directory = root / f"{backend}-{batch}-{mode}"
                if binary:
                    run_case(binary, directory, backend, batch, mode)
                else:
                    write_case(directory)
    if binary is None:
        print(f"Prepared 8 cases in {root}; no SQL was executed.")


if __name__ == "__main__":
    main()
