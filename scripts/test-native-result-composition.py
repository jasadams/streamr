#!/usr/bin/env python3
"""Qualify existing-SQL lifetime and latest closed-HOP composition.

The query uses only native HOP, updating aggregates, UNION ALL, COALESCE and
HAVING. It checks values and recovery, including the watermark-driven
quiet-key zero: a retained key's recent count advances from 1 to 0 once an
event-time watermark passes its expiry boundary, while the lifetime count and
the key stay. Silence with no watermark progress emits nothing. Prepare
fixtures without --binary; with a fresh SQL test binary, run memory/RocksDB,
controller/leader, source batch 1/8.
"""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess


BASE = datetime.fromisoformat("2023-10-09T17:13:20")
SLIDE = 2
WIDTH = 4
FIELDS = {"k", "lifetime_count", "recent_count"}

# (key, offset seconds, value); HOP(slide 2, width 4) membership per row.
MAIN_EVENTS = (("x", 1, 1), ("x", 3, 2), ("x", 7, 3))
# keyA goes quiet after offset 7; keyB's later events advance the watermark
# past keyA's expiry boundary at 12 (window [6,10) start 6 + width 4 + slide 2).
QUIET_EVENTS = (
    ("keyA", 1, 1), ("keyA", 3, 2), ("keyA", 7, 3),
    ("keyB", 11, 4), ("keyB", 13, 5),
)
# keyC is removed from the retaining key relation; keyD stays. Update-mode
# sources reject event-time fields, so the retractable key relation is a
# separate changelog source while the rolling windows keep event time on the
# append source. Removing the retaining key must delete its composed result.
DELETE_EVENTS = (("keyC", 1, 1), ("keyC", 3, 2), ("keyD", 5, 6), ("keyD", 9, 7))
DELETE_KEYS = ("keyC", "keyD")
DELETE_KEY_REMOVAL = "keyC"

SCENARIOS = {
    "main": MAIN_EVENTS,
    "quiet-key": QUIET_EVENTS,
    "quiet-key-silence": QUIET_EVENTS,
    "delete-key": DELETE_EVENTS,
}


def window_counts(events) -> dict[str, dict[int, int]]:
    windows: dict[str, dict[int, int]] = {}
    for key, second, _ in events:
        pane = second // SLIDE * SLIDE
        for start in (pane - SLIDE, pane):
            assert start <= second < start + WIDTH
            windows.setdefault(key, {})
            windows[key][start] = windows[key].get(start, 0) + 1
    return windows


def latest_counts(events) -> dict[str, int]:
    return {key: counts[max(counts)] for key, counts in window_counts(events).items()}


def lifetime_counts(events) -> dict[str, int]:
    totals: dict[str, int] = {}
    for key, _, _ in events:
        totals[key] = totals.get(key, 0) + 1
    return totals


def expiry_deadline(events) -> dict[str, int]:
    # Start-stamped retention of width + slide reaches the first empty closed
    # boundary after the latest nonempty window of a key.
    deadlines = {}
    for key, counts in window_counts(events).items():
        deadlines[key] = max(counts) + WIDTH + SLIDE
    return deadlines


def independent_oracle(scenario: str) -> None:
    events = SCENARIOS[scenario]
    windows = window_counts(events)
    latest = latest_counts(events)
    totals = lifetime_counts(events)
    deadlines = expiry_deadline(events)
    assert sum(sum(counts.values()) for counts in windows.values()) == 2 * len(events)
    for key, counts in windows.items():
        assert totals[key] == sum(
            1 for event_key, _, _ in events if event_key == key
        )
        assert deadlines[key] == max(counts) + WIDTH + SLIDE
    if scenario.startswith("quiet-key"):
        assert totals == {"keyA": 3, "keyB": 2}
        assert latest == {"keyA": 1, "keyB": 1}
        # keyA's quiet result [6,10) expires at 12, before keyB's offset-13
        # event is observed at a batch-1 watermark; keyB retires at 18.
        assert deadlines == {"keyA": 12, "keyB": 18}
    if scenario == "main":
        assert totals == {"x": 3} and latest == {"x": 1} and deadlines == {"x": 12}
    if scenario == "delete-key":
        assert totals == {"keyC": 2, "keyD": 2} and latest == {"keyC": 1, "keyD": 1}
        assert set(DELETE_KEYS) == {"keyC", "keyD"} and DELETE_KEY_REMOVAL == "keyC"


def query_sql(directory: Path, watermark: str) -> str:
    return f"""
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp AS {watermark})
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE composed_out (k TEXT, lifetime_count BIGINT, recent_count BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW lifetime AS SELECT k, COUNT(*) AS lifetime_count
  FROM events GROUP BY k;
CREATE VIEW rolling AS SELECT k,
  HOP(INTERVAL '{SLIDE} seconds', INTERVAL '{WIDTH} seconds') AS window,
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
  COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k
  HAVING MAX(lifetime_count) IS NOT NULL;
"""


def deletion_query_sql(directory: Path) -> str:
    return f"""
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp AS timestamp)
WITH (connector = 'single_file', path = '{directory}/input.jsonl',
      format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE key_relation (k TEXT NOT NULL PRIMARY KEY, v BIGINT)
WITH (connector = 'single_file', path = '{directory}/keys.jsonl',
      format = 'debezium_json', type = 'source', wait_for_control = 'true');
CREATE TABLE composed_out (k TEXT, lifetime_count BIGINT, recent_count BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW lifetime AS SELECT k, COUNT(*) AS lifetime_count
  FROM key_relation GROUP BY k;
CREATE VIEW rolling AS SELECT k,
  HOP(INTERVAL '{SLIDE} seconds', INTERVAL '{WIDTH} seconds') AS window,
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
  COALESCE(MAX(recent_count), 0) AS recent_count FROM normalized GROUP BY k
  HAVING MAX(lifetime_count) IS NOT NULL;
"""


def write_fixture(directory: Path, scenario: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    events = SCENARIOS[scenario]
    if scenario == "delete-key":
        (directory / "input.jsonl").write_text("".join(
            json.dumps({
                "timestamp": (BASE + timedelta(seconds=second)).isoformat(),
                "k": key, "v": value,
            }) + "\n" for key, second, value in events
        ))
        key_rows = [
            {"before": None, "after": {"k": key, "v": index}, "op": "c", "ts_ms": 0}
            for index, key in enumerate(DELETE_KEYS)
        ] + [
            {"before": {"k": DELETE_KEY_REMOVAL, "v": 0}, "after": None, "op": "d",
             "ts_ms": 0}
        ]
        (directory / "keys.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in key_rows)
        )
    else:
        rows = [
            {
                "timestamp": (BASE + timedelta(seconds=second)).isoformat(),
                "k": key, "v": value,
            }
            for key, second, value in events
        ]
        (directory / "input.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
    if scenario == "delete-key":
        sql = deletion_query_sql(directory)
    else:
        watermark = (
            "timestamp" if scenario.startswith("quiet-key") else
            "timestamp - INTERVAL '1 minute'"
        )
        sql = query_sql(directory, watermark)
    (directory / "query.sql").write_text(sql)
    finals = {
        "main": {"x": {"lifetime_count": 3, "recent_count": 0}},
        "quiet-key": {
            "keyA": {"lifetime_count": 3, "recent_count": 0},
            "keyB": {"lifetime_count": 2, "recent_count": 0},
        },
        "quiet-key-silence": {
            "keyA": {"lifetime_count": 3, "recent_count": 0},
            "keyB": {"lifetime_count": 2, "recent_count": 0},
        },
        "delete-key": {"keyD": {"lifetime_count": 1, "recent_count": 0}},
    }[scenario]
    (directory / "expected.final.json").write_text(
        json.dumps(finals, indent=2, sort_keys=True) + "\n"
    )


def reduce_cdc(path: Path, scenario: str, allow_deletes: bool):
    """Strict per-key CDC reduce; returns the per-key row chains."""
    chains: dict[str, list[dict]] = {}
    current: dict[str, dict] = {}
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
            if op == "d":
                assert allow_deletes and after is None, (path, count, row)
            else:
                assert after is not None, (path, count, row)
            key = (after or before)["k"]
            assert set(after or before) == FIELDS, (path, count, row)
            assert before == current.get(key), (path, count, before, current.get(key))
            if op == "u":
                assert before != after, (path, count, row)
            if after is not None:
                lifetime = after["lifetime_count"]
                recent = after["recent_count"]
                assert type(lifetime) is int and lifetime >= 1, (path, count, after)
                assert type(recent) is int and recent >= 0, (path, count, after)
                totals = lifetime_counts(SCENARIOS[scenario])
                if scenario == "delete-key":
                    assert lifetime <= 1, (path, count, after)
                else:
                    assert lifetime <= totals[key], (path, count, after)
                if before is not None and op == "u":
                    assert lifetime >= before["lifetime_count"], (path, count, after)
                # Recent is the zero-extended current result: 0 once the
                # retained result expired or before the first closed window.
                allowed = set(window_counts(SCENARIOS[scenario])[key].values()) | {0}
                assert recent in allowed, (path, count, after)
                current[key] = after
            else:
                del current[key]
            chains.setdefault(key, []).append(row)
    return chains, current, count


def assert_quiet_key_zero(chains: dict[str, list[dict]], scenario: str) -> None:
    # The recorded generic output must advance keyA from lifetime3/recent1 to
    # lifetime3/recent0 with no synthetic event for the key: every keyA source
    # row precedes the replacement.
    replacements = [
        (index, row)
        for index, row in enumerate(chains["keyA"])
        if row["op"] == "u"
        and row["before"] == {"k": "keyA", "lifetime_count": 3, "recent_count": 1}
        and row["after"] == {"k": "keyA", "lifetime_count": 3, "recent_count": 0}
    ]
    assert len(replacements) == 1, (scenario, chains["keyA"])
    assert chains["keyA"][-1]["after"] == {
        "k": "keyA", "lifetime_count": 3, "recent_count": 0,
    }, (scenario, chains["keyA"][-1])
    assert chains["keyB"][-1]["after"] == {
        "k": "keyB", "lifetime_count": 2, "recent_count": 0,
    }, (scenario, chains["keyB"][-1])


def assert_silence_hold(pre: Path, post: Path) -> None:
    # Wall-clock waiting without watermark progress must not advance the
    # event-time result; the snapshots around the hold are identical.
    assert pre.read_text() == post.read_text(), (pre, post)


def run_case(binary: Path, directory: Path, scenario: str, backend: str, batch: int,
             mode: str) -> None:
    write_fixture(directory, scenario)
    stale = ["output.jsonl", "output.initial.jsonl", "capture.log"]
    for phase in ("initial", "recovered"):
        for boundary in ("before", "after"):
            stale.append(f"output.idle-{phase}-{boundary}.jsonl")
    for name in stale:
        (directory / name).unlink(missing_ok=True)
    events = SCENARIOS[scenario]
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
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="3"
        if scenario != "main" else "1",
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="1",
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="1",
        # Each upstream aggregate can flush once at its initial tick and once
        # at EOF; expiry replacements add one transition per retained key.
        STREAMR_CAPTURE_MAX_INITIAL_ROWS="8",
        STREAMR_CAPTURE_EXPECTED_ROWS="2",
        STREAMR_CAPTURE_MAX_ROWS="16",
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    if scenario == "quiet-key-silence":
        env.update(
            STREAMR_CAPTURE_IDLE_SOURCE_ROW_TARGET="3",
            STREAMR_CAPTURE_IDLE_SECONDS="8",
            STREAMR_CAPTURE_IDLE_MIN_PRE_ROWS="1",
            STREAMR_CAPTURE_IDLE_MAX_BYTES="262144",
            STREAMR_CAPTURE_IDLE_PRE_MATCH_POINTER="/after/k",
            STREAMR_CAPTURE_IDLE_PRE_MATCH_VALUE=json.dumps("keyA"),
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
    allow_deletes = scenario == "delete-key"
    initial = directory / "output.initial.jsonl"
    chains, current, count = reduce_cdc(initial, scenario, allow_deletes)
    finals = json.loads((directory / "expected.final.json").read_text())
    if initial.exists() and count:
        assert current == finals, (initial, current, finals)
    chains, current, count = reduce_cdc(directory / "output.jsonl", scenario, allow_deletes)
    assert current == finals, (directory, current, finals)
    if scenario.startswith("quiet-key"):
        assert_quiet_key_zero(chains, scenario)
    if scenario == "delete-key":
        assert "keyC" not in current and any(
            row["op"] == "d" for row in chains["keyC"]
        ), chains["keyC"]
    if scenario == "quiet-key-silence":
        for phase in ("initial", "recovered"):
            pre = directory / f"output.idle-{phase}-before.jsonl"
            post = directory / f"output.idle-{phase}-after.jsonl"
            if pre.exists() and post.exists():
                assert_silence_hold(pre, post)
    print(f"PASS {directory.name}: {scenario} lifetime/current CDC and expiry", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path, help="fresh arroyo-sql-testing test binary")
    parser.add_argument(
        "--scenario", choices=sorted(SCENARIOS),
        help="run one scenario; default runs every scenario",
    )
    args = parser.parse_args()
    directory = args.directory.resolve()
    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    for scenario in scenarios:
        independent_oracle(scenario)
    if args.binary is None:
        for scenario in scenarios:
            write_fixture(directory / scenario, scenario)
        print(f"Prepared {directory} for {scenarios}")
        return
    binary = args.binary.resolve(strict=True)
    for scenario in scenarios:
        for backend in ("memory", "rocksdb"):
            # The harness idle hold only supports one-row source batches.
            batches = (1,) if scenario == "quiet-key-silence" else (1, 8)
            for batch in batches:
                for mode in ("controller", "leader"):
                    run_case(
                        binary, directory / scenario / f"{backend}-{batch}-{mode}",
                        scenario, backend, batch, mode,
                    )


if __name__ == "__main__":
    main()
