#!/usr/bin/env python3
"""Direct-source FIRST/LAST differential for native and legacy updating state.

Equal ORDER BY tuples have no deterministic LAST result: report observed native
and legacy outcomes without requiring equality. `--total-order` adds `amount`
as a tie breaker and requires both paths to match an independent exact oracle.
"""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess

EVENTS = [
    dict(tenant_id="a", amount=5, seq=1), dict(tenant_id="b", amount=7, seq=1),
    dict(tenant_id="a", amount=None, seq=2), dict(tenant_id="b", amount=3, seq=2),
    dict(tenant_id="a", amount=10, seq=3), dict(tenant_id="b", amount=9, seq=3),
    dict(tenant_id="a", amount=8, seq=3), dict(tenant_id="b", amount=4, seq=3),
]
CHECKPOINT = {
    "a": dict(tenant_id="a", events=2, earliest=5, latest=None),
    "b": dict(tenant_id="b", events=2, earliest=7, latest=3),
}
FINAL_STABLE = {
    "a": dict(tenant_id="a", events=4, earliest=5),
    "b": dict(tenant_id="b", events=4, earliest=7),
}
FINAL_TOTAL = {
    "a": dict(tenant_id="a", events=4, earliest=5, latest=10),
    "b": dict(tenant_id="b", events=4, earliest=7, latest=9),
}
TIED_FINAL_VALUES = {"a": {8, 10}, "b": {4, 9}}

def write_fixture(directory: Path, native: bool, total_order: bool) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text("".join(json.dumps(row) + "\n" for row in EVENTS))
    retention = "NULL" if native else "INTERVAL '24 hours'"
    ordering = "seq, amount" if total_order else "seq"
    (directory / "query.sql").write_text(f"""
SET updating_ttl = {retention};
CREATE TABLE tie_input (tenant_id TEXT NOT NULL, amount BIGINT, seq BIGINT NOT NULL)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE tie_output (tenant_id TEXT, events BIGINT, earliest BIGINT, latest BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'debezium_json', type = 'sink');
CREATE VIEW tie_result AS SELECT tenant_id,
  COUNT(*) AS events,
  FIRST_VALUE(amount ORDER BY {ordering}) IGNORE NULLS AS earliest,
  LAST_VALUE(amount ORDER BY {ordering}) AS latest
  FROM tie_input GROUP BY tenant_id;
INSERT INTO tie_output SELECT tenant_id, events, earliest, latest FROM tie_result;
""")

def prefix_values(prefix, total_order=False):
    result = {}
    for key in {event["tenant_id"] for event in EVENTS[:prefix]}:
        rows = [event for event in EVENTS[:prefix] if event["tenant_id"] == key]
        first_seq = min(row["seq"] for row in rows if row["amount"] is not None)
        earliest = next(row["amount"] for row in rows
                        if row["seq"] == first_seq and row["amount"] is not None)
        last_seq = max(row["seq"] for row in rows)
        latest = {row["amount"] for row in rows if row["seq"] == last_seq}
        if total_order and None not in latest:
            latest = {max(latest)}
        result[key] = [dict(tenant_id=key, events=len(rows), earliest=earliest, latest=value)
                       for value in latest]
    return result

def reduce(path: Path, stable: dict, total_order=False, prefix_count=None) -> tuple[list, dict]:
    current, actions, positions = {}, [], {}
    prefixes = [prefix_values(index, total_order)
                for index in range(1, (prefix_count or len(EVENTS)) + 1)]
    for line in path.read_text().splitlines():
        row = json.loads(line)
        row = row.get("payload", row)
        assert set(row) >= {"before", "after", "op"}, row
        before, after, op = row["before"], row["after"], row["op"]
        # These fixtures are append-only: groups never disappear.
        assert op in {"c", "u"}, row
        assert (before is None) == (op == "c"), row
        assert (after is None) == (op == "d"), row
        value = after if after is not None else before
        assert set(value) == {"tenant_id", "events", "earliest", "latest"}, value
        key = value["tenant_id"]
        assert current.get(key) == before, (key, current.get(key), before)
        assert type(value["events"]) is int, value
        if after is not None:
            candidates = [index for index, snapshot in enumerate(prefixes)
                          if index >= positions.get(key, -1)
                          and after in snapshot.get(key, [])]
            assert candidates, ("not a forward source-prefix value", after)
            positions[key] = candidates[0]
        if after is None:
            del current[key]
        else:
            current[key] = after
        actions.append((key, op))
    assert set(current) == set(stable), (current, stable)
    for key, expected in stable.items():
        for field, wanted in expected.items():
            assert current[key][field] == wanted, (key, field, current[key], wanted)
    return actions, current

def run_case(binary: Path, directory: Path, batch: int, native: bool, total_order: bool):
    write_fixture(directory, native, total_order)
    env = dict(
        os.environ,
        STREAMR_TEST_BACKEND="memory",
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_CHECKPOINT_MODE="controller",
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT="4",
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS="2",
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="2",
        STREAMR_CAPTURE_EXPECTED_ROWS="2",
        STREAMR_CAPTURE_MAX_INITIAL_ROWS=str(len(EVENTS)),
        STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS="4",
        STREAMR_CAPTURE_MAX_ROWS=str(len(EVENTS)),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    if native:
        env["STREAMR_TEST_NATIVE_AGGREGATES"] = "1"
    else:
        env.pop("STREAMR_TEST_NATIVE_AGGREGATES", None)
    log = directory / "capture.log"
    with log.open("w") as output:
        result = subprocess.run(
            [str(binary), "external_sql_checkpoint_capture", "--ignored", "--test-threads=1", "--nocapture"],
            env=env, stdout=output, stderr=subprocess.STDOUT,
        )
    if result.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"{directory}: capture failed; inspect {log}")
    final_expected = FINAL_TOTAL if total_order else FINAL_STABLE
    initial_actions, initial = reduce(directory / "output.initial.jsonl", final_expected, total_order)
    assert 2 <= len(initial_actions) <= len(EVENTS), initial_actions
    records = (directory / "output.jsonl").read_text().splitlines()
    markers = re.findall(
        r'^CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=4 '
        r'committed_rows=(\d+) rows=(\d+) bytes=\d+ path=(.+) job=\S+$',
        log.read_text(), re.MULTILINE)
    assert len(markers) == 1, markers
    committed, captured, output_path = markers[0]
    committed, captured = int(committed), int(captured)
    assert output_path == str(directory / "output.jsonl"), output_path
    assert 2 <= committed <= 4, committed
    assert committed <= len(records) == captured <= len(EVENTS), (committed, captured)
    prefix = directory / "checkpoint.prefix.jsonl"
    prefix.write_text("\n".join(records[:committed]) + "\n")
    checkpoint_actions, checkpoint = reduce(prefix, CHECKPOINT, total_order, prefix_count=4)
    recovered_actions, recovered = reduce(directory / "output.jsonl", final_expected, total_order)
    assert len(checkpoint_actions) == committed, checkpoint_actions
    assert len(recovered_actions) == captured, recovered_actions
    assert recovered_actions[:committed] == checkpoint_actions
    if not total_order:
        for result in (initial, recovered):
            for key, allowed in TIED_FINAL_VALUES.items():
                assert result[key]["latest"] in allowed, (key, result[key], allowed)
    print(f"PASS {directory.name}: initial={initial}, checkpoint={checkpoint}, recovered={recovered}", flush=True)
    return initial, checkpoint, recovered

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--total-order", action="store_true", help="ORDER BY seq, amount with exact final oracle")
    args = parser.parse_args()
    root = args.directory.resolve()
    if not args.binary:
        write_fixture(root, True, args.total_order)
        print(f"Prepared {root}; supply --binary to run the differential")
        return
    binary = args.binary.resolve(strict=True)
    for batch in (1, 8):
        native = run_case(binary, root / f"native-memory-{batch}-controller", batch, True, args.total_order)
        legacy = run_case(binary, root / f"legacy-memory-{batch}-controller", batch, False, args.total_order)
        if args.total_order:
            assert native == legacy, (batch, "total-order FIRST/LAST differential", native, legacy)
            print(f"PASS batch={batch}: native and legacy match independent total-order oracle", flush=True)
        elif native != legacy:
            print(f"OBSERVED batch={batch}: equal ORDER BY tuples are underspecified; native={native}, legacy={legacy}", flush=True)
        else:
            print(f"PASS batch={batch}: equal-order outcomes happened to match", flush=True)

if __name__ == "__main__":
    main()
