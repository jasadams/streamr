#!/usr/bin/env python3
"""Candidate generic two-stage updating aggregate fixture and independent oracle.

This prepares data only. The parent runner supplies its own Bookworm test binary,
backend, checkpoint mode and source batch size. Equal ORDER BY ties should also
be compared against the existing operator in the same batch configuration.
"""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess


EVENTS = [
    dict(tenant_id="a", item_id=1, amount=5, seq=1),
    dict(tenant_id="a", item_id=2, amount=None, seq=2),
    dict(tenant_id="b", item_id=1, amount=7, seq=3),
    dict(tenant_id="b", item_id=2, amount=3, seq=4),
    dict(tenant_id="a", item_id=1, amount=10, seq=5),
    dict(tenant_id="a", item_id=2, amount=4, seq=6),
    dict(tenant_id="b", item_id=1, amount=2, seq=7),
    dict(tenant_id="a", item_id=3, amount=8, seq=6),
    dict(tenant_id="b", item_id=2, amount=9, seq=9),
    dict(tenant_id="a", item_id=1, amount=None, seq=10),
    dict(tenant_id="b", item_id=3, amount=None, seq=9),
]
FIELDS = ("tenant_id", "items", "positive_items", "total", "lo", "hi", "earliest", "latest")


def expected(prefix, deterministic=True):
    per_item = {}
    ordinal = 0
    for event in EVENTS[:prefix]:
        key = event["tenant_id"], event["item_id"]
        old = per_item.get(key)
        values = [event["amount"]]
        if old is not None:
            values.append(old["amount"])
        nonnull = [value for value in values if value is not None]
        amount = max(nonnull) if nonnull else None
        sort_seq = max(event["seq"], old["sort_seq"] if old else event["seq"])
        if old is None or (amount, sort_seq) != (old["amount"], old["sort_seq"]):
            ordinal += 1
            per_item[key] = dict(
                item_id=event["item_id"], amount=amount,
                sort_seq=sort_seq, ordinal=ordinal,
            )
    result = {}
    for tenant in sorted({key[0] for key in per_item}):
        rows = [row for (owner, _), row in per_item.items() if owner == tenant]
        amounts = [row["amount"] for row in rows if row["amount"] is not None]
        positive = sum(value > 0 for value in amounts)
        # FIRST_VALUE IGNORE NULLS chooses the first non-null active item.
        if deterministic:
            ordered = sorted(rows, key=lambda row: (row["sort_seq"], row["item_id"]))
            latest = ordered[-1]["amount"]
        else:
            ordered = sorted(rows, key=lambda row: (row["sort_seq"], row["ordinal"]))
            # Candidate stable tie policy. Compare to the existing operator
            # separately because equal ORDER BY tuples do not define a result.
            latest = sorted(rows, key=lambda row: (-row["sort_seq"], row["ordinal"]))[0]["amount"]
        earliest = next((row["amount"] for row in ordered if row["amount"] is not None), None)
        result[tenant] = dict(
            tenant_id=tenant,
            items=len(rows),
            positive_items=positive,
            total=sum(amounts) if amounts else None,
            lo=min(amounts) if amounts else None,
            hi=max(amounts) if amounts else None,
            earliest=earliest,
            latest=latest,
        )
    return result


def tied_latest_values(prefix):
    """All legal LAST_VALUE members among active items with maximal sort_seq."""
    groups = {}
    for event in EVENTS[:prefix]:
        groups.setdefault((event["tenant_id"], event["item_id"]), []).append(event)
    active = {}
    for (tenant, _), rows in groups.items():
        amounts = [row["amount"] for row in rows if row["amount"] is not None]
        active.setdefault(tenant, []).append((max(row["seq"] for row in rows),
                                              max(amounts) if amounts else None))
    return {tenant: {amount for seq, amount in members
                     if seq == max(position for position, _ in members)}
            for tenant, members in active.items()}


def strict_reduce(path, target, compare_tied_latest=True, prefix_count=None):
    """Validate every Debezium transition, not just a final-row projection."""
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    current = {}
    actions = []
    positions = {}
    prefixes = [expected(index, compare_tied_latest) for index in range(1, (prefix_count or len(EVENTS)) + 1)]
    tied = [tied_latest_values(index) for index in range(1, len(prefixes) + 1)]
    for record in rows:
        row = record.get("payload", record)
        assert set(row) >= {"before", "after", "op"}, row
        before, after, op = row["before"], row["after"], row["op"]
        # These fixtures are append-only: groups never disappear.
        assert op in {"c", "u"}, row
        assert (before is None) == (op == "c"), row
        assert (after is None) == (op == "d"), row
        value = after if after is not None else before
        assert set(value) == set(FIELDS), value
        key = value["tenant_id"]
        assert current.get(key) == before, (key, before, current.get(key), row)
        if after is not None:
            assert key in target, key
            candidate = {field: val for field, val in after.items()
                         if compare_tied_latest or field != "latest"}
            candidates = [index for index, snapshot in enumerate(prefixes)
                          if index >= positions.get(key, -1) and key in snapshot
                          and {field: val for field, val in snapshot[key].items()
                               if compare_tied_latest or field != "latest"} == candidate
                          and (compare_tied_latest or after["latest"] in tied[index][key])]
            assert candidates, ("not a forward source-prefix aggregate", after)
            positions[key] = candidates[0]
        if after is None:
            del current[key]
        else:
            current[key] = after
        actions.append((key, op))
    if compare_tied_latest:
        assert current == target, (current, target)
    else:
        assert set(current) == set(target), (current, target)
        for key in target:
            expected_other = {field: value for field, value in target[key].items() if field != "latest"}
            actual_other = {field: value for field, value in current[key].items() if field != "latest"}
            assert actual_other == expected_other, (key, actual_other, expected_other)
            assert current[key]["latest"] in tied[-1][key], (key, current[key], tied[-1][key])
    return actions, current


def write_fixture(directory, equal_order, native):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in EVENTS)
    )
    for name, count in (("checkpoint", 4), ("final", len(EVENTS))):
        (directory / f"expected.{name}.json").write_text(
            json.dumps(expected(count, not equal_order), ensure_ascii=False, indent=2) + "\n"
        )
    order = "sort_seq" if equal_order else "sort_seq, item_id"
    retention = "SET updating_ttl = NULL;" if native else "SET updating_ttl = INTERVAL '24 hours';"
    query = f"""
{retention}
CREATE TABLE aggregate_input (tenant_id TEXT NOT NULL, item_id BIGINT NOT NULL, amount BIGINT, seq BIGINT NOT NULL)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE aggregate_output (tenant_id TEXT, items BIGINT, positive_items BIGINT, total BIGINT,
  lo BIGINT, hi BIGINT, earliest BIGINT, latest BIGINT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'debezium_json', type = 'sink');
CREATE VIEW per_item AS SELECT tenant_id, item_id,
  MAX(amount) AS amount, MAX(seq) AS sort_seq
  FROM aggregate_input GROUP BY tenant_id, item_id;
CREATE VIEW per_tenant AS SELECT tenant_id,
  COUNT(*) AS items,
  COUNT(amount) FILTER (WHERE amount > 0) AS positive_items,
  SUM(amount) AS total,
  MIN(amount) AS lo,
  MAX(amount) AS hi,
  FIRST_VALUE(amount ORDER BY {order}) IGNORE NULLS AS earliest,
  LAST_VALUE(amount ORDER BY {order}) AS latest
  FROM per_item GROUP BY tenant_id;
INSERT INTO aggregate_output SELECT tenant_id, items, positive_items, total, lo, hi, earliest, latest
  FROM per_tenant;
"""
    (directory / "query.sql").write_text(query)


def run_case(binary, directory, backend, batch, mode, equal_order, native):
    write_fixture(directory, equal_order, native)
    environment = dict(
        os.environ,
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600",
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=mode,
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
        environment["STREAMR_TEST_NATIVE_AGGREGATES"] = "1"
    else:
        environment.pop("STREAMR_TEST_NATIVE_AGGREGATES", None)
    log = directory / "capture.log"
    with log.open("w") as output:
        completed = subprocess.run(
            [str(binary), "external_sql_checkpoint_capture", "--ignored", "--test-threads=1", "--nocapture"],
            env=environment, stdout=output, stderr=subprocess.STDOUT,
        )
    if completed.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"{directory} capture failed; inspect {log}")
    final = expected(len(EVENTS), not equal_order)
    checkpoint = expected(4, not equal_order)
    compare_tie = not equal_order
    initial_actions, initial = strict_reduce(
        directory / "output.initial.jsonl", final, compare_tie
    )
    assert 2 <= len(initial_actions) <= len(EVENTS), initial_actions
    recovered_path = directory / "output.jsonl"
    records = recovered_path.read_text().splitlines()
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
    checkpoint_path = directory / "checkpoint.prefix.jsonl"
    checkpoint_path.write_text("\n".join(records[:committed]) + "\n")
    checkpoint_actions, checkpoint_actual = strict_reduce(checkpoint_path, checkpoint, compare_tie, prefix_count=4)
    assert len(checkpoint_actions) == committed, checkpoint_actions
    recovered_actions, recovered = strict_reduce(recovered_path, final, compare_tie)
    assert len(recovered_actions) == captured, recovered_actions
    assert recovered_actions[:committed] == checkpoint_actions
    print(f"PASS {directory.name}: initial={initial} checkpoint={checkpoint_actual} recovered={recovered}", flush=True)
    return initial, checkpoint_actual, recovered


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path, help="fresh arroyo-sql-testing test executable")
    parser.add_argument("--baseline", action="store_true", help="also run existing memory operator at batch 1/8")
    parser.add_argument("--check", type=Path, help="reduce an actual Debezium JSONL output")
    parser.add_argument("--prefix", type=int, default=len(EVENTS))
    parser.add_argument(
        "--equal-order", action="store_true",
        help="omit item_id tie-breaker for differential legacy/native comparison",
    )
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not args.binary:
        write_fixture(directory, args.equal_order, True)
        if args.check:
            print(strict_reduce(args.check, expected(args.prefix, not args.equal_order), not args.equal_order, prefix_count=args.prefix))
        else:
            print(
                f"Prepared {directory}; checkpoint={expected(4, not args.equal_order)}, "
                f"final={expected(len(EVENTS), not args.equal_order)}"
            )
        return
    binary = args.binary.resolve(strict=True)
    outcomes = {}
    for backend in ("memory", "rocksdb"):
        for batch in (1, 8):
            for mode in ("controller", "leader"):
                case = f"native-{backend}-{batch}-{mode}"
                outcomes[(backend, batch, mode)] = run_case(
                    binary, directory / case, backend, batch, mode, args.equal_order, True
                )
    if args.baseline:
        for batch in (1, 8):
            case = f"baseline-memory-{batch}-controller"
            baseline = run_case(
                binary, directory / case, "memory", batch, "controller", args.equal_order, False
            )
            native = outcomes[("memory", batch, "controller")]
            assert baseline[0] == native[0], (batch, "initial", baseline[0], native[0])
            assert baseline[1] == native[1], (batch, "checkpoint", baseline[1], native[1])
            assert baseline[2] == native[2], (batch, "recovered", baseline[2], native[2])
            print(f"PASS differential batch={batch}", flush=True)


if __name__ == "__main__":
    main()
