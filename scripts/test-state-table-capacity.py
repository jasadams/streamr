#!/usr/bin/env python3
"""Qualify all-key SQL state-table checkpoint recovery under a declared RSS limit.

Use the rebuilt Bookworm SQL-testing executable. Small fixtures remain selectable;
--require-10x admits only configurations whose checkpoint AND full retained payload
floors reach ten times the fixed 48 MiB pool sum. No other operator/live/fault claim.
"""
import argparse
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import re
import subprocess

_spec = importlib.util.spec_from_file_location(
    "window_capacity", Path(__file__).with_name("test-native-window-capacity.py"))
capacity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capacity)
WORKER_BUDGET_MIB = 48


def payload_for(item, size):
    return hashlib.shake_256(str(item).encode()).hexdigest((size + 1) // 2)[:size]


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def host_source_snapshot(repo, evidence, binary):
    build = json.loads(evidence.read_text())
    declared = dict(build["compiled_sources"])
    declared.update(build.get("workspace_build_files", {}))
    declared.update(build.get("reviewed_source", {}).get("rust_sources", {}))
    files = {}
    workspace = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rust-toolchain", "build.rs"}
    for name, expected in declared.items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or not (
                name.startswith(("crates/", ".cargo/")) or name in workspace):
            raise ValueError("invalid host compiled-source path")
        actual = digest(repo / path)
        if actual != expected:
            raise ValueError(f"mounted compiled source differs from host build evidence: {name}")
        files[name] = actual
    for name in ("Cargo.toml", "Cargo.lock", "crates/arroyo-sql-testing/src/smoke_tests.rs"):
        if name not in files:
            raise ValueError(f"host build evidence lacks required workspace/capture source: {name}")
    for name in workspace:
        if (repo / name).is_file():
            files[name] = digest(repo / name)
    if (repo / ".cargo").is_dir():
        for path in (repo / ".cargo").rglob("*"):
            if path.is_file():
                files[str(path.relative_to(repo))] = digest(path)
    if binary and digest(binary) != build["sql_test_sha256"]:
        raise ValueError("SQL test binary differs from host build evidence")
    return dict(source_revision=build["repository_head"],
                source_compiled_diff_sha256=build["compiled_diff_sha256"],
                mounted_compiled_files=files, host_build_evidence_sha256=digest(evidence))


def expected_row(index, rows):
    insert = index < rows
    return dict(event_id=index, item_id=index if insert else index - rows,
                action="insert" if insert else "none",
                old_quantity=None if insert else 1, new_quantity=1,
                old_matches=None if insert else True,
                new_matches=True, lookup_matches=True)


def check_output(path, rows, prefix=None):
    expected_count = 2 * rows if prefix is None else prefix
    count = 0
    with path.open() as captured:
        lines = captured if prefix is None else itertools.islice(captured, prefix)
        for count, line in enumerate(lines, 1):
            index = count - 1
            if index >= expected_count:
                raise AssertionError(f"{path}: extra row {index}")
            actual = json.loads(line)
            expected = expected_row(index, rows)
            # Exact JSON types as well as field names, values and source order.
            if json.dumps(actual, sort_keys=True) != json.dumps(expected, sort_keys=True):
                raise AssertionError(f"{path} row {index}: {actual} != {expected}")
    if count != expected_count:
        raise AssertionError(f"{path}: {count} rows != {expected_count}")


def query(input_path, output_path):
    input_sql = str(input_path).replace("'", "''")
    output_sql = str(output_path).replace("'", "''")
    return f"""
CREATE TABLE input_events (event_id BIGINT NOT NULL, item_id BIGINT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL)
WITH (connector='single_file', path='{input_sql}', format='json', type='source', wait_for_control='true');
CREATE TABLE output_events (event_id BIGINT, item_id BIGINT, action TEXT, old_quantity BIGINT, new_quantity BIGINT, old_matches BOOLEAN, new_matches BOOLEAN, lookup_matches BOOLEAN)
WITH (connector='single_file', path='{output_sql}', format='json', type='sink');
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", type=Path)
    parser.add_argument("--directory", type=Path, default=Path("/app/target/state-table-capacity"))
    parser.add_argument("--rows", type=int, default=65000)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--checkpoint-rows", type=int,
                        help="Insertion prefix checkpointed; default rows minus one")
    parser.add_argument("--rss-limit-mib", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--protocol", choices=("controller", "leader"), action="append")
    parser.add_argument("--require-10x", action="store_true",
                        help="Reject before preparation unless checkpoint and full payload floors reach 10x")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--source-evidence", type=Path,
                        help="Host build snapshot when container worktree git metadata is unavailable")
    args = parser.parse_args()
    if min(args.rows, args.payload_bytes, args.rss_limit_mib, args.timeout_seconds) <= 0:
        parser.error("all sizes, counts and timeouts must be positive")
    if args.rows < 2:
        parser.error("at least two rows are needed for checkpoint/replay")
    checkpoint = args.rows - 1 if args.checkpoint_rows is None else args.checkpoint_rows
    if not 0 < checkpoint < args.rows:
        parser.error("checkpoint rows must be a positive proper insertion prefix")
    if not args.prepare_only and not args.binary:
        parser.error("binary is required unless --prepare-only is selected")
    full_bytes = args.rows * args.payload_bytes
    checkpoint_bytes = checkpoint * args.payload_bytes
    threshold = 10 * WORKER_BUDGET_MIB * 1024 * 1024
    if args.require_10x and min(full_bytes, checkpoint_bytes) < threshold:
        parser.error("both full and checkpoint retained payload must reach 10x declared pool sum")
    binary = args.binary.resolve(strict=True) if args.binary else None
    repo = Path(__file__).resolve().parent.parent
    evidence = args.source_evidence.resolve(strict=True) if args.source_evidence else None
    if evidence:
        source_provenance = host_source_snapshot(repo, evidence, binary)
    else:
        try:
            revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo,
                                      capture_output=True, text=True, check=True).stdout.strip()
            source_diff = subprocess.run(["git", "diff", "HEAD", "--binary"], cwd=repo,
                                         capture_output=True, check=True).stdout
        except subprocess.CalledProcessError as error:
            parser.error("worktree git metadata unavailable; supply --source-evidence with the verified host build snapshot")
        source_provenance = dict(source_revision=revision,
                                source_tracked_diff_sha256=hashlib.sha256(source_diff).hexdigest())
    root = args.directory.resolve()
    if root.exists() and any(root.iterdir()):
        parser.error("directory must be new or empty; preserve existing fixtures and validation evidence")
    root.mkdir(parents=True, exist_ok=True)
    input_path = root / "input.jsonl"
    # Each key is probed after restore. This checks complete independently
    # encoded values for every checkpointed key, not only 64 sampled keys.
    with input_path.open("w") as output:
        for mode in ("put", "probe"):
            for item in range(args.rows):
                output.write(json.dumps(dict(event_id=item + (args.rows if mode == "probe" else 0),
                    item_id=item, mode=mode, payload=payload_for(item, args.payload_bytes))) + "\n")
    # Streaming independently declared oracle; runtime output never supplies it.
    expected_path = root / "expected.jsonl"
    with expected_path.open("w") as expected:
        for index in range(2 * args.rows):
            expected.write(json.dumps(expected_row(index, args.rows)) + "\n")
    provenance = dict(**source_provenance, input_sha256=digest(input_path),
                      oracle_sha256=digest(expected_path),
                      driver_sha256=digest(Path(__file__).resolve()),
                      child_runner_sha256=digest(Path(capacity.__file__).resolve()),
                      capture_harness_sha256=digest(repo / "crates/arroyo-sql-testing/src/smoke_tests.rs"),
                      binary_sha256=digest(binary) if binary else None)
    settings = dict(rows=args.rows, input_rows=2*args.rows, all_key_probe_rows=args.rows,
                    checkpoint_rows=checkpoint, logical_payload_bytes=full_bytes,
                    checkpoint_retained_payload_floor_bytes=checkpoint_bytes,
                    declared_worker_budget_mib=WORKER_BUDGET_MIB,
                    pool_budget_mib=dict(execution=16, block_cache=8, memtable=4,
                                         queued_write=2, decoded=16, scan=2),
                    full_state_exceeds_10x_pool_budget=full_bytes>=threshold,
                    checkpoint_state_exceeds_10x_pool_budget=checkpoint_bytes>=threshold,
                    require_10x=args.require_10x, rss_limit_mib=args.rss_limit_mib,
                    timeout_seconds=args.timeout_seconds, provenance=provenance,
                    oracle_scope="exact per-event order/action/OLD/NEW/current lookup; full deterministic payload equality for every key")
    (root / "fixture.json").write_text(json.dumps(settings, indent=2)+"\n")
    measurements = []
    for mode in dict.fromkeys(args.protocol or ("controller", "leader")):
        directory = root / mode
        directory.mkdir()
        output_path = directory / "output.jsonl"
        query_path = directory / "query.sql"
        query_path.write_text(query(input_path, output_path))
        if args.prepare_only:
            print(f"PREPARED {query_path}: {args.rows} keys, checkpoint={checkpoint}, all-key probes={args.rows}", flush=True)
            continue
        environment = dict(os.environ)
        for name in list(environment):
            if name.startswith("STREAMR_CAPTURE_") or name.startswith("STREAMR_TEST_"):
                environment.pop(name)
        environment.update(STREAMR_TEST_EXECUTION_BYTES="16777216", STREAMR_TEST_TYPED_SQL="1",
            STREAMR_TEST_SOURCE_BATCH_ROWS="32", STREAMR_TEST_BACKEND="rocksdb",
            STREAMR_TEST_CHECKPOINT_MODE=mode, STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(args.timeout_seconds),
            STREAMR_CAPTURE_QUERY=str(query_path), STREAMR_CAPTURE_OUTPUT=str(output_path),
            STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(checkpoint),
            STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(2*args.rows),
            STREAMR_CAPTURE_EXPECTED_ROWS=str(2*args.rows),
            STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(checkpoint), STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
        log_path = directory / "runtime.log"
        pinned = {str(path): digest(path) for path in (input_path, expected_path, query_path,
                  binary, Path(__file__).resolve(), Path(capacity.__file__).resolve(),
                  repo / "crates/arroyo-sql-testing/src/smoke_tests.rs")}
        if evidence:
            pinned[str(evidence)] = digest(evidence)
        print(f"RUN {mode}: full retained payload={full_bytes}, checkpoint={checkpoint_bytes}, "
              f"declared pool={WORKER_BUDGET_MIB}MiB, RSS limit={args.rss_limit_mib}MiB", flush=True)
        status, peak_bytes = capacity.child_with_usage(
            [str(binary), "external_sql_checkpoint_capture", "--ignored", "--test-threads=1", "--nocapture"],
            environment, log_path, args.timeout_seconds)
        if any(digest(Path(path)) != expected for path, expected in pinned.items()):
            raise ValueError("source/binary/query/input/oracle/evidence changed during state-table capture")
        if evidence and host_source_snapshot(repo, evidence, binary) != source_provenance:
            raise ValueError("mounted compiled-source host snapshot changed during capture")
        log = log_path.read_text()
        if status or "1 passed" not in log:
            raise RuntimeError(f"{mode} failed ({status}): {log_path}")
        for filename in ("output.initial.jsonl", "output.jsonl"):
            check_output(directory / filename, args.rows)
        committed = re.findall(r"^CAPTURE_RESULT phase=recovered checkpoint=\d+ "
            r"input_rows_before_checkpoint=\d+ committed_rows=(\d+) ", log, re.MULTILINE)
        if len(committed) != 1 or int(committed[0]) != checkpoint:
            raise AssertionError(f"missing/inconsistent committed prefix metadata: {log_path}")
        check_output(output_path, args.rows, prefix=int(committed[0]))
        if peak_bytes > args.rss_limit_mib * 1024 * 1024:
            raise RuntimeError(f"{mode} exceeded RSS envelope: {peak_bytes} bytes")
        measurement = dict(settings, mode=mode, peak_rss_bytes=peak_bytes,
                           emitted_rows=2*args.rows, restored_prefix_keys_probed=checkpoint,
                           committed_prefix_values_verified=True, query_sha256=digest(query_path),
                           output_initial_sha256=digest(directory / "output.initial.jsonl"),
                           output_recovered_sha256=digest(output_path), capture_log_sha256=digest(log_path))
        measurements.append(measurement)
        (root / "measurements.json").write_text(json.dumps(measurements, indent=2)+"\n")
        print(f"PASS {mode}: exact initial/recovered/prefix comparisons, all {args.rows} keys probed; "
              f"RSS={peak_bytes}; checkpoint-10x={checkpoint_bytes>=threshold}", flush=True)


if __name__ == "__main__":
    main()
