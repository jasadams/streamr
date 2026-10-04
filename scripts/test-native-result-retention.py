#!/usr/bin/env python3
"""Probe retained winner state for existing-SQL lifetime + closed-HOP UNION.

Generates only caller-defined generic rows. With --binary, runs one capture case;
with --inspector, inventories its explicitly selected checkpoint. The `many`
scenario checkpoints on the first event of a following group, after 64 or 4096
closed groups; it tests retained winner/member shape but not thousands of outer
CDC replacements because the 3600-second flush coalesces updates. The
`retraction` scenario checkpoints after nine real events at 1, 9, 17, 25, 33,
41, 49, 49.5, 57 seconds, then restores for a 65-second event. In batch-8
mode the first batch has watermark 1 second, while the ninth row is its own
batch with watermark 57 seconds. Both batch-1 and batch-8 therefore close the
49/49.5-second HOP count of 2 before the barrier; the suffix closes a count of
1, forcing one outer MAX replacement. Neither scenario claims an
autonomous idle-zero emission. Multiple settled barriers would be needed to
prove repeated outer retractions over many checkpoints.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

BASE = datetime(2023, 10, 9, 17, 13, 20, tzinfo=timezone.utc)
FIELDS = {"k", "lifetime_count", "recent_count"}


def input_rows(groups, scenario):
    if scenario == "retraction":
        return [1000, 9000, 17000, 25000, 33000, 41000,
                49000, 49500, 57000, 65000], 9
    assert scenario == "many" and groups in (64, 4096), (scenario, groups)
    rows = []
    for group in range(groups):
        base_ms = group * 8000
        rows.append(base_ms + 1000)
        # Source timing can flush a final batch at any length from 1 to 8.
        # Keep the last eight real history groups double so every possible
        # last-batch minimum leaves a closed count-2 window at the barrier.
        if group % 2 or group >= groups - 8:
            rows.append(base_ms + 1500)
    prefix = len(rows) + 1
    rows.append(groups * 8000 + 1000)  # closes last full group before barrier
    rows.append((groups + 1) * 8000 + 1000)  # closes following group after restore
    return rows, prefix


def window_counts(rows):
    windows = {}
    for milliseconds in rows:
        pane = milliseconds // 2000 * 2000
        for start in (pane - 2000, pane):
            assert start <= milliseconds < start + 4000
            windows[start] = windows.get(start, 0) + 1
    return windows


def latest_closed(rows, watermark_ms):
    windows = window_counts(rows)
    closed = {start + 4000: count for start, count in windows.items()
              if start + 4000 < watermark_ms}
    assert closed, (len(rows), watermark_ms)
    return closed[max(closed)]


def prefix_watermark(rows, prefix, batch_rows):
    # The current WatermarkGenerator forwards the minimum watermark expression
    # within each emitted source batch, then retains the maximum seen so far.
    return max(min(rows[start:min(prefix, start + batch_rows)])
               for start in range(0, prefix, batch_rows))


def expected_values(groups, scenario):
    rows, prefix = input_rows(groups, scenario)
    before = {"k": "x", "lifetime_count": prefix,
              "recent_count": latest_closed(rows[:prefix], rows[prefix - 1])}
    # EOF delivers the terminal watermark; windows ending at the final event
    # are then eligible for emission. The checkpoint uses its actual event WM.
    after = {"k": "x", "lifetime_count": len(rows),
             "recent_count": latest_closed(rows, 2**63 - 1)}
    assert before["recent_count"] == 2 and after["recent_count"] == 1
    if scenario == "many":
        assert prefix == groups * 3 // 2 + 5 and len(rows) == prefix + 1
        # Any observed final source batch has at most the configured eight
        # rows. Its minimum is the last-batch watermark on sorted input;
        # prove the exact latest closed window for every possible partition.
        assert all(latest_closed(rows[:prefix], rows[prefix - last_batch]) == 2
                   for last_batch in range(1, 9))
    else:
        assert prefix == 9 and len(rows) == 10
        assert all(prefix_watermark(rows, prefix, batch) == 57000
                   for batch in (1, 8))
        assert all(latest_closed(rows[:prefix], prefix_watermark(rows, prefix, batch)) == 2
                   for batch in (1, 8))
    return rows, prefix, before, after


def write_fixture(directory, groups, scenario):
    directory.mkdir(parents=True, exist_ok=True)
    rows, prefix, before, after = expected_values(groups, scenario)
    with (directory / "input.jsonl").open("w") as output:
        for index, milliseconds in enumerate(rows):
            timestamp = (BASE + timedelta(milliseconds=milliseconds)).replace(tzinfo=None)
            output.write(json.dumps({"timestamp": timestamp.isoformat(),
                                     "k": "x", "v": index + 1}) + "\n")
    (directory / "query.sql").write_text(f"""
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp AS timestamp)
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
    (directory / "expected.checkpoint.json").write_text(json.dumps(before, indent=2) + "\n")
    (directory / "expected.final.json").write_text(json.dumps(after, indent=2) + "\n")
    return prefix, before, after


def strict_reduce(path, minimum, maximum, final, possible_recent,
                  committed_rows=None, checkpoint=None, expected_records=None):
    current = None
    records = 0
    replaced_recent = False
    with path.open() as source:
        for line in source:
            records += 1
            row = json.loads(line)
            assert isinstance(row, dict), (path, records, row)
            payload = row.get("payload", row)
            assert isinstance(payload, dict) and {"before", "after", "op"} <= payload.keys()
            before, after, operation = payload["before"], payload["after"], payload["op"]
            assert operation in {"c", "u", "d"}, (path, records, payload)
            assert (before is None) == (operation == "c"), (path, records, payload)
            assert operation != "d" and after is not None, (path, records, payload)
            assert before == current, (path, records, before, current)
            if operation == "u":
                assert before != after, (path, records, payload)
            assert isinstance(after, dict) and set(after) == FIELDS, (path, records, after)
            assert after["k"] == "x"
            lifetime, recent = after["lifetime_count"], after["recent_count"]
            assert lifetime is not None or recent is not None, (path, records, after)
            assert lifetime is None or (
                type(lifetime) is int and 1 <= lifetime <= final["lifetime_count"]
            ), (path, records, after)
            assert recent is None or (
                type(recent) is int and recent in possible_recent
            ), (path, records, after)
            if current is not None:
                old_lifetime = current["lifetime_count"]
                if old_lifetime is not None:
                    assert lifetime is not None and lifetime >= old_lifetime, (path, records, after)
                if current["recent_count"] is not None:
                    assert recent is not None, (path, records, after)
                if committed_rows is not None and records > committed_rows:
                    replaced_recent |= current["recent_count"] == 2 and recent == 1
            current = after
            if committed_rows is not None and records == committed_rows:
                assert current == checkpoint, (path, records, current, checkpoint)
    assert minimum <= records <= maximum, (path, records, minimum, maximum)
    if expected_records is not None:
        assert records == expected_records, (path, records, expected_records)
    if committed_rows is not None:
        assert committed_rows < records <= committed_rows + 4, (
            path, committed_rows, records
        )
        assert replaced_recent, (path, committed_rows, records)
    assert current == final, (path, current, final)


def inspect_checkpoint(script, root, job, mode, directory):
    result = subprocess.run([sys.executable, str(script), "--storage-root", str(root),
                             "--job-id", job, "--epoch", "1", "--mode", mode,
                             "--expected-owner-count", "3"],
                            capture_output=True, text=True, check=True)
    inventory = json.loads(result.stdout)
    owners = inventory["aggregate_owners"]
    counts = [owner["prefix_counts"] for owner in owners]
    assert all(all(count[k] == 0 for k in ("D", "E", "C")) for count in counts)
    assert sorted((count["G"], count["M"], count["R"]) for count in counts) == [
        (1, 0, 0), (1, 0, 0), (1, 4, 4)], counts
    for owner in owners:
        group = owner["value_lengths"].get("G")
        assert group and group["count"] == 1 and 0 < group["max"] <= 32 * 1024
    (directory / "checkpoint-inventory.json").write_text(
        json.dumps(inventory, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--scenario", choices=("many", "retraction"), default="many")
    parser.add_argument("--groups", type=int, choices=(64, 4096),
                        help="required for the many-window scenario")
    parser.add_argument("--binary", type=Path, help="fresh SQL testing executable")
    parser.add_argument("--backend", choices=("memory", "rocksdb"), default="memory")
    parser.add_argument("--batch", type=int, choices=(1, 8), default=1)
    parser.add_argument("--mode", choices=("controller", "leader"), default="controller")
    parser.add_argument("--checkpoint-root", type=Path,
                        help="persistent mounted local checkpoint directory inside test container")
    parser.add_argument("--inspector", type=Path,
                        help="read-only checkpoint_inventory.py path inside test container")
    args = parser.parse_args()
    if args.scenario == "many":
        assert args.groups in (64, 4096), "--groups 64 or 4096 required"
    else:
        assert args.groups is None, "--groups applies only to the many-window scenario"
    directory = args.directory.resolve()
    prefix, before, after = write_fixture(directory, args.groups, args.scenario)
    if not args.binary:
        print(f"Prepared scenario={args.scenario} groups={args.groups} prefix={prefix}: "
              f"checkpoint={before}, final={after}")
        return
    assert args.checkpoint_root, "--checkpoint-root required with --binary"
    checkpoint_root = args.checkpoint_root.resolve()
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    for name in ("output.jsonl", "output.initial.jsonl", "capture.log",
                 "checkpoint-inventory.json"):
        (directory / name).unlink(missing_ok=True)
    env = dict(os.environ)
    env.pop("STREAMR_TEST_TYPED_SQL", None)
    env.update(
        ARROYO__CHECKPOINT_URL=checkpoint_root.as_uri(),
        STREAMR_TEST_NATIVE_AGGREGATES="1",
        STREAMR_TEST_NATIVE_WINDOWS="1",
        STREAMR_TEST_MAX_OPEN_DATABASES="4",
        STREAMR_TEST_MAX_SNAPSHOTS="4",
        STREAMR_TEST_SCAN_PAGE_BYTES="4194304",
        # Three 2 MiB aggregate scopes and a 512 KiB window scope reserve
        # over 32 MiB together, including encoded-write copies and metadata.
        STREAMR_TEST_QUEUED_WRITE_BYTES="67108864",
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_BACKEND=args.backend,
        STREAMR_TEST_CHECKPOINT_MODE=args.mode,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(args.batch),
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(prefix),
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
        # One immediate startup tick plus EOF/barrier per native aggregate can
        # expose at most four distinct branch contributions to the outer key.
        STREAMR_CAPTURE_MAX_INITIAL_ROWS="4",
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="1",
        STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS="4",
        STREAMR_CAPTURE_EXPECTED_ROWS="2",
        STREAMR_CAPTURE_MAX_ROWS="8",
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    log = directory / "capture.log"
    with log.open("w") as output:
        result = subprocess.run([str(args.binary.resolve(strict=True)),
                                 "external_sql_checkpoint_capture", "--ignored",
                                 "--test-threads=1", "--nocapture"],
                                env=env, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"capture failed: {log}")
    log_text = log.read_text()
    initial_match = re.search(r"CAPTURE_RESULT phase=initial rows=(\d+)\b", log_text)
    recovered_match = re.search(
        r"CAPTURE_RESULT phase=recovered .* committed_rows=(\d+) rows=(\d+)\b .* job=([^\s]+)",
        log_text,
    )
    assert initial_match and recovered_match, f"missing capture row metadata: {log}"
    initial_rows = int(initial_match.group(1))
    committed_rows = int(recovered_match.group(1))
    recovered_rows = int(recovered_match.group(2))
    assert 1 <= initial_rows <= 4 and 1 <= committed_rows <= 4
    assert committed_rows + 1 <= recovered_rows <= committed_rows + 4
    possible_recent = set(window_counts(input_rows(args.groups, args.scenario)[0]).values())
    assert possible_recent == {1, 2}
    strict_reduce(directory / "output.initial.jsonl", 1, 4, after,
                  possible_recent, expected_records=initial_rows)
    strict_reduce(directory / "output.jsonl", 2, 8, after, possible_recent,
                  committed_rows=committed_rows, checkpoint=before,
                  expected_records=recovered_rows)
    if args.inspector:
        inspect_checkpoint(args.inspector.resolve(strict=True), checkpoint_root,
                           recovered_match.group(3), args.mode, directory)
    print(f"PASS {args.scenario} groups={args.groups} {args.backend} "
          f"batch{args.batch} {args.mode}: "
          "exact CDC/recovery" + (" and selected checkpoint live member shape" if args.inspector else ""))


if __name__ == "__main__":
    main()
