#!/usr/bin/env python3
"""Exercise retained SQL state and selected-checkpoint restore under a declared RSS limit.

Use the rebuilt Bookworm arroyo-sql-testing executable. Small --rows runs check
the fixture only; the 10x flag requires retained payload >= 10x the fixed fixture pool budget.
The measured RSS envelope includes baseline engine/runtime memory separately. This does not qualify other operators or the full live gate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--directory", type=Path, default=Path("/app/target/state-table-capacity"))
    parser.add_argument("--rows", type=int, default=65000)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--rss-limit-mib", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if min(args.rows, args.payload_bytes,
           args.rss_limit_mib, args.timeout_seconds) <= 0:
        parser.error("all sizes, counts and timeouts must be positive")
    if args.rows < 2:
        parser.error("at least two rows are needed for checkpoint/replay")
    binary = str(args.binary.resolve(strict=True))
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=True)
    # Conservative sum of the fixed fixture's executor/live pool limits. The
    # memtable is also charged to block cache, so this sum overstates its budget.
    worker_budget_mib = 48
    def payload_for(item):
        return hashlib.shake_256(str(item).encode()).hexdigest((args.payload_bytes + 1) // 2)[:args.payload_bytes]
    probes = min(64, args.rows)
    selected = [i * (args.rows - 1) // (probes - 1) for i in range(probes)]
    input_path = root / "input.jsonl"
    # Stream fixture preparation: the generator must not retain the whole input.
    with input_path.open("w") as output:
        for item in range(args.rows):
            output.write(json.dumps(dict(event_id=item, item_id=item, mode="put", payload=payload_for(item))) + "\n")
        for offset, item in enumerate(selected):
            output.write(json.dumps(dict(event_id=args.rows + offset, item_id=item, mode="probe", payload=payload_for(item))) + "\n")
    expected_rows = args.rows + probes
    logical_bytes = args.rows * args.payload_bytes
    exceeds_pool_budget = logical_bytes >= 10 * worker_budget_mib * 1024 * 1024
    measurements = []
    for mode in ("controller", "leader"):
        directory = root / mode
        directory.mkdir(exist_ok=True)
        output_path = directory / "output.jsonl"
        query = f"""
CREATE TABLE input_events (event_id BIGINT NOT NULL, item_id BIGINT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL)
WITH (connector='single_file', path='{input_path}', format='json', type='source', wait_for_control='true');
CREATE TABLE output_events (event_id BIGINT, item_id BIGINT, action TEXT, old_quantity BIGINT, new_quantity BIGINT, old_matches BOOLEAN, new_matches BOOLEAN, lookup_matches BOOLEAN)
WITH (connector='single_file', path='{output_path}', format='json', type='sink');
CREATE STATE TABLE inventory (item_id BIGINT PRIMARY KEY, quantity BIGINT, payload TEXT) PARTITION BY item_id;
CREATE VIEW applied AS MERGE INTO inventory AS target USING input_events AS source
ON target.item_id=source.item_id
WHEN NOT MATCHED AND source.mode='put' THEN INSERT (item_id, quantity, payload) VALUES (source.item_id, 1, source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output_events SELECT r.source.event_id AS event_id, r.source.item_id AS item_id,
r.action AS action, r.old.quantity AS old_quantity, r.new.quantity AS new_quantity,
r.old.payload = r.source.payload AS old_matches, r.new.payload = r.source.payload AS new_matches,
i.payload = r.source.payload AS lookup_matches
FROM applied r LEFT JOIN inventory i ON i.item_id=r.source.item_id;
"""
        query_path = directory / "query.sql"
        query_path.write_text(query)
        environment = dict(os.environ,
            STREAMR_TEST_EXECUTION_BYTES="16777216",
            STREAMR_TEST_TYPED_SQL="1",
            STREAMR_TEST_SOURCE_BATCH_ROWS="32",
            STREAMR_TEST_BACKEND="rocksdb",
            STREAMR_TEST_CHECKPOINT_MODE=mode,
            STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(args.timeout_seconds),
            STREAMR_CAPTURE_QUERY=str(query_path),
            STREAMR_CAPTURE_OUTPUT=str(output_path),
            STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(args.rows // 2),
            STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(expected_rows),
            STREAMR_CAPTURE_EXPECTED_ROWS=str(expected_rows),
            STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(args.rows // 2),
            STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
        )
        log_path = directory / "runtime.log"
        print(f"RUN {mode}: retained payload={logical_bytes}, declared worker budget={worker_budget_mib}MiB, RSS limit={args.rss_limit_mib}MiB", flush=True)
        with log_path.open("w") as log:
            process = subprocess.Popen(
                [binary, "external_sql_checkpoint_capture", "--ignored", "--test-threads=1", "--nocapture"],
                env=environment, stdout=log, stderr=subprocess.STDOUT,
            )
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        if process.returncode or "1 passed" not in log_path.read_text():
            raise RuntimeError(f"{mode} failed: {process.returncode}; inspect {log_path}")
        for filename in ("output.initial.jsonl", "output.jsonl"):
            count = 0
            with (directory / filename).open() as captured:
                for count, line in enumerate(captured, 1):
                    index = count - 1
                    if index >= expected_rows:
                        raise RuntimeError(f"extra row in {mode}/{filename}")
                    insert = index < args.rows
                    expected = dict(
                        event_id=index,
                        item_id=index if insert else selected[index - args.rows],
                        action="insert" if insert else "none",
                        old_quantity=None if insert else 1,
                        new_quantity=1,
                        old_matches=None if insert else True,
                        new_matches=True,
                        lookup_matches=True,
                    )
                    actual = json.loads(line)
                    if actual != expected:
                        raise RuntimeError(f"{mode}/{filename} row {index}: {actual} != {expected}")
            if count != expected_rows:
                raise RuntimeError(f"{mode}/{filename}: {count} rows != {expected_rows}")
        # Linux wait4 reports this child's maximum RSS in KiB, including all of
        # the worker's threads, rather than one sample at checkpoint time.
        peak_bytes = usage.ru_maxrss * 1024
        if peak_bytes > args.rss_limit_mib * 1024 * 1024:
            raise RuntimeError(f"{mode} exceeded declared RSS envelope: {peak_bytes} bytes")
        measurements.append(dict(mode=mode, logical_payload_bytes=logical_bytes,
                                 declared_worker_budget_mib=worker_budget_mib,
                                 rss_limit_mib=args.rss_limit_mib, peak_rss_bytes=peak_bytes,
                                 rows=expected_rows, exceeds_pool_budget=exceeds_pool_budget))
        print(f"PASS {mode}: two complete output comparisons, peak RSS={peak_bytes}, exceeds_10x_pool_budget={exceeds_pool_budget}", flush=True)
    (root / "measurements.json").write_text(json.dumps(measurements, indent=2) + "\n")


if __name__ == "__main__":
    main()
