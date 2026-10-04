#!/usr/bin/env python3
"""RocksDB native TUMBLE/SESSION checkpoint capacity and exact-output fixture.

Small --rows runs validate the fixture. The middle checkpoint retains only
rows//2 payloads; full pre-EOF state holds all rows. A 10x result is eligible
only for full pre-EOF state that reaches ten times the fixed, conservative
50 MiB pool sum. RSS includes the whole SQL-test process. No rescaling is tested.
"""

import argparse
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


BASE = datetime.fromisoformat("2023-10-09T17:13:20")
POOL_BUDGET_MIB = 50  # executor 16 + cache 8 + memtable 4 + write 4 + decoded 16 + scan 2
CASES = ("tumble", "session")
PROTOCOLS = ("controller", "leader")
BACKENDS = ("memory", "rocksdb")


def payload_for(item, size):
    # Each item has independent, reproducible content. Hex is ASCII, so its
    # character count is also its byte count in Arrow Utf8 and JSON strings.
    return hashlib.shake_256(str(item).encode()).hexdigest((size + 1) // 2)[:size]


def write_input(path, rows, payload_bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as output:
        for item in range(rows):
            output.write(json.dumps(dict(
                timestamp=BASE.isoformat(), item_id=item, segment="hot",
                ordinal=item, payload=payload_for(item, payload_bytes),
            ), separators=(",", ":")) + "\n")


def query(case, input_path, output_path):
    source = f"""
CREATE TABLE capacity_input (
  timestamp TIMESTAMP NOT NULL, item_id BIGINT NOT NULL,
  segment TEXT NOT NULL, ordinal BIGINT NOT NULL, payload TEXT NOT NULL,
  WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute')
WITH (connector='single_file', path='{input_path}', format='json',
      type='source', wait_for_control='true');
"""
    if case == "tumble":
        return source + f"""
CREATE TABLE capacity_output (
  item_id BIGINT, start TIMESTAMP, end TIMESTAMP, n BIGINT,
  first_payload TEXT)
WITH (connector='single_file', path='{output_path}', format='json', type='sink');
INSERT INTO capacity_output
SELECT item_id, window.start, window.end, n, first_payload FROM (
  SELECT item_id, TUMBLE(INTERVAL '2 second') AS window,
         COUNT(*) AS n,
         FIRST_VALUE(payload ORDER BY ordinal) AS first_payload
  FROM capacity_input GROUP BY 1, 2
);
"""
    if case == "session":
        return source + f"""
CREATE TABLE capacity_output (
  segment TEXT, start TIMESTAMP, end TIMESTAMP, n BIGINT,
  first_payload TEXT)
WITH (connector='single_file', path='{output_path}', format='json', type='sink');
INSERT INTO capacity_output
SELECT segment, window.start, window.end, n, first_payload FROM (
  SELECT segment, SESSION(INTERVAL '10 seconds') AS window,
         COUNT(*) AS n,
         FIRST_VALUE(payload ORDER BY ordinal) AS first_payload
  FROM capacity_input GROUP BY segment, window
);
"""
    raise ValueError(case)


def prepare(root, cases, protocols, backends, rows, payload_bytes):
    input_path = root / "input.jsonl"
    write_input(input_path, rows, payload_bytes)
    for case in cases:
        for backend in backends:
            for protocol in protocols:
                directory = root / f"{case}-{backend}-{protocol}"
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "query.sql").write_text(query(
                    case, input_path, directory / "output.jsonl"))
    return input_path


def child_with_usage(command, env, log_path, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    with log_path.open("w") as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        while True:
            pid, status, usage = os.wait4(child.pid, os.WNOHANG)
            if pid:
                child.returncode = os.waitstatus_to_exitcode(status)
                return child.returncode, usage.ru_maxrss * 1024
            if time.monotonic() >= deadline:
                child.kill()
                os.wait4(child.pid, 0)
                raise TimeoutError(f"SQL capture exceeded {timeout_seconds}s: {log_path}")
            time.sleep(0.2)


def check_output(path, case, rows, payload_bytes):
    common = {
        "tumble": (BASE.isoformat(), (BASE + timedelta(seconds=2)).isoformat()),
        "session": (BASE.isoformat(), (BASE + timedelta(seconds=10)).isoformat()),
    }
    expected_start, expected_end = common[case]
    fields = {"start", "end", "n", "first_payload"}
    if case == "tumble":
        fields.add("item_id")
        seen = bytearray(rows)
        count = 0
        with path.open() as output:
            for line in output:
                row = json.loads(line)
                if set(row) != fields:
                    raise AssertionError((path, "field set", set(row), fields))
                item = row["item_id"]
                if type(item) is not int or not 0 <= item < rows or seen[item]:
                    raise AssertionError((path, "duplicate/invalid item", item))
                expected_payload = payload_for(item, payload_bytes)
                wanted = dict(item_id=item, start=expected_start, end=expected_end,
                              n=1, first_payload=expected_payload)
                if row != wanted:
                    raise AssertionError((path, "wrong value", item, row, wanted))
                seen[item] = 1
                count += 1
        if count != rows or not all(seen):
            raise AssertionError((path, "missing rows", count, rows))
        return count
    fields.add("segment")
    wanted = dict(segment="hot", start=expected_start, end=expected_end,
                  n=rows, first_payload=payload_for(0, payload_bytes))
    with path.open() as output:
        first = output.readline()
        second = output.readline()
    if not first or second:
        raise AssertionError((path, "expected exactly one hot session"))
    actual = json.loads(first)
    if set(actual) != fields or actual != wanted:
        raise AssertionError((path, "wrong hot session", actual, wanted))
    return 1


def run_case(binary, directory, case, backend, protocol, rows, payload_bytes,
             rss_limit_mib, timeout_seconds):
    output_path = directory / "output.jsonl"
    env = dict(os.environ)
    for flag in ("STREAMR_TEST_TYPED_SQL", "STREAMR_TEST_NATIVE_AGGREGATES"):
        env.pop(flag, None)
    env.update(
        STREAMR_TEST_NATIVE_WINDOWS="1",
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol,
        STREAMR_TEST_SOURCE_BATCH_ROWS="32",
        STREAMR_TEST_EXECUTION_BYTES=str(16 * 1024 * 1024),
        STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(timeout_seconds),
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(output_path),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(rows // 2),
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(rows if case == "tumble" else 1),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS="0",
        STREAMR_CAPTURE_EXPECTED_ROWS=str(rows if case == "tumble" else 1),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    log_path = directory / "runtime.log"
    print(f"RUN {case}/{backend}/{protocol}: {rows} retained rows, "
          f"{payload_bytes} payload bytes each", flush=True)
    status, peak_rss = child_with_usage(
        [str(binary), "external_sql_checkpoint_capture", "--ignored",
         "--test-threads=1", "--nocapture"], env, log_path, timeout_seconds)
    if status or "1 passed" not in log_path.read_text():
        raise RuntimeError(f"{case}/{backend}/{protocol} capture failed ({status}): {log_path}")
    initial = check_output(directory / "output.initial.jsonl", case, rows, payload_bytes)
    recovered = check_output(output_path, case, rows, payload_bytes)
    if peak_rss > rss_limit_mib * 1024 * 1024:
        raise RuntimeError(f"{case}/{backend}/{protocol} peak RSS {peak_rss} exceeds {rss_limit_mib} MiB")
    checkpoint_bytes = (rows // 2) * payload_bytes
    full_open_bytes = rows * payload_bytes
    threshold = 10 * POOL_BUDGET_MIB * 1024 * 1024
    result = dict(case=case, backend=backend, protocol=protocol,
                  checkpoint_retained_payload_floor_bytes=checkpoint_bytes,
                  full_open_state_payload_floor_bytes=full_open_bytes,
                  declared_pool_budget_mib=POOL_BUDGET_MIB,
                  checkpoint_state_exceeds_10x_pool_budget=checkpoint_bytes >= threshold,
                  full_state_exceeds_10x_pool_budget=full_open_bytes >= threshold,
                  rss_limit_mib=rss_limit_mib,
                  peak_rss_bytes=peak_rss, initial_rows=initial, recovered_rows=recovered,
                  oracle_scope=("all emitted keys and full payloads" if case == "tumble"
                                else "hot-session count, window, and full first payload"))
    print(f"PASS {case}/{backend}/{protocol}: initial/recovered exact outputs; "
          f"RSS={peak_rss} bytes; full-open-10x={full_open_bytes >= threshold}; "
          f"checkpoint-10x={checkpoint_bytes >= threshold}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", type=Path,
                        help="Fresh arroyo-sql-testing executable; omit with --prepare-only")
    parser.add_argument("--directory", type=Path,
                        default=Path("/app/target/native-window-capacity"))
    parser.add_argument("--rows", type=int, default=65000)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--rss-limit-mib", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--case", choices=CASES, action="append")
    parser.add_argument("--protocol", choices=PROTOCOLS, action="append")
    parser.add_argument("--backend", choices=BACKENDS, action="append",
                        help="Default RocksDB; memory is intended for small smoke runs")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if min(args.rows, args.payload_bytes, args.rss_limit_mib, args.timeout_seconds) <= 0:
        parser.error("all counts, sizes and timeouts must be positive")
    if args.rows < 2:
        parser.error("at least two rows are needed for an open checkpoint")
    if not args.prepare_only and not args.binary:
        parser.error("binary is required unless --prepare-only is set")
    root = args.directory.resolve()
    cases = tuple(dict.fromkeys(args.case or CASES))
    protocols = tuple(dict.fromkeys(args.protocol or PROTOCOLS))
    backends = tuple(dict.fromkeys(args.backend or ("rocksdb",)))
    prepare(root, cases, protocols, backends, args.rows, args.payload_bytes)
    checkpoint_bytes = (args.rows // 2) * args.payload_bytes
    full_open_bytes = args.rows * args.payload_bytes
    print(f"PREPARED {root}: checkpoint retained floor={checkpoint_bytes} bytes; "
          f"full pre-EOF retained floor={full_open_bytes} bytes; "
          f"conservative pool sum={POOL_BUDGET_MIB} MiB; "
          f"full-open-10x={full_open_bytes >= 10 * POOL_BUDGET_MIB * 1024 * 1024}",
          flush=True)
    if args.prepare_only:
        return
    binary = args.binary.resolve(strict=True)
    results = []
    for case in cases:
        for backend in backends:
            for protocol in protocols:
                result = run_case(binary, root / f"{case}-{backend}-{protocol}",
                                  case, backend, protocol, args.rows, args.payload_bytes,
                                  args.rss_limit_mib, args.timeout_seconds)
                results.append(result)
                (root / "measurements.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
