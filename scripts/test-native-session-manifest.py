#!/usr/bin/env python3
"""Run caller-declared SQL SESSION fixtures through real full-checkpoint capture.

No oracle is derived from captured output. A manifest supplies SQL, input,
expected complete rows, checkpoint cardinality and optional idle observations.
--prepare writes neutral qualification fixtures; omit --binary to only prepare.
Ordered/reused-key cases default to both backends and checkpoint protocols;
ordered-reuse also runs source batches 1 and 8. Capacity cases retain RocksDB as
the default and require separate actual retained-row checkpoint measurement
with scripts/verify-retained-checkpoint-rows.py before qualifying payload size.
"""
import argparse
import itertools
from bounded_row_oracle import ArrayRows, exact_rows
from datetime import datetime, timedelta
import importlib.util
import json
import os
import re
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "window_capacity", Path(__file__).with_name("test-native-window-capacity.py"))
capacity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capacity)
BASE = datetime(2023, 10, 9, 17, 13, 20)


def iso(seconds):
    return (BASE + timedelta(seconds=seconds)).isoformat()


def prepare(directory, scenario, keys, payload_bytes, hot_rows=65001):
    directory.mkdir(parents=True, exist_ok=True)
    input_path = directory / "input.jsonl"
    expected = []
    total_rows = 0
    gap = 10
    collections = scenario in ("ordered-reuse", "closed-reuse")
    with input_path.open("w") as output:
        def emit(key, second, ordinal, payload):
            nonlocal total_rows
            row = dict(timestamp=iso(second), k=key, ordinal=ordinal,
                       metric=ordinal + 1, attribute=payload)
            if collections:
                row["selected"] = payload != ""
            output.write(json.dumps(row) + "\n")
            total_rows += 1
        if scenario == "many":
            # Two raw rows per open session, independently encoded payloads.
            # Interleave keys so the owner cannot finish one key before another.
            for ordinal in range(2):
                for key in range(keys):
                    emit(str(key), 0, ordinal,
                         capacity.payload_for(key * 2 + ordinal, payload_bytes))
            for key in range(keys):
                expected.append(dict(k=str(key), start=iso(0), end=iso(10), n=2,
                    total=3, lo=1, hi=2, first_attribute=capacity.payload_for(key*2,payload_bytes),
                    last_attribute=capacity.payload_for(key*2+1,payload_bytes)))
        elif scenario == "hot":
            for ordinal in range(hot_rows):
                emit("hot", ordinal * 9, ordinal,
                     capacity.payload_for(ordinal, payload_bytes))
            expected.append(dict(k="hot", start=iso(0), end=iso((hot_rows-1)*9+gap),
                n=hot_rows, total=hot_rows*(hot_rows+1)//2, lo=1, hi=hot_rows,
                first_attribute=capacity.payload_for(0,payload_bytes),
                last_attribute=capacity.payload_for(hot_rows-1,payload_bytes)))
        elif scenario == "continuous":
            # A native event-time session crosses both gap*100 and 24h without
            # an invented maximum-duration or processing-time closure policy.
            count = 9602
            for ordinal in range(count):
                emit("continuous", ordinal * 9, ordinal, f"value-{ordinal}")
            expected.append(dict(k="continuous", start=iso(0), end=iso((count-1)*9+gap),
                n=count, total=count*(count+1)//2, lo=1, hi=count,
                first_attribute="value-0", last_attribute=f"value-{count-1}"))
        elif scenario == "closed-reuse":
            events = [("reused", 0, "first"), ("reused", 9, ""),
                      ("other", 30, "other-first"), ("reused", 40, "reused-first"),
                      ("reused", 49, "reused-last"), ("other", 70, "other-reused")]
            for ordinal, (key, second, attribute) in enumerate(events):
                emit(key, second, ordinal, attribute)
            expected = [dict(k="reused", start=iso(0), end=iso(19), n=2,
                             total=3, lo=1, hi=2, first_attribute="first", last_attribute="first"),
                        dict(k="other", start=iso(30), end=iso(40), n=1,
                             total=3, lo=3, hi=3, first_attribute="other-first", last_attribute="other-first"),
                        dict(k="reused", start=iso(40), end=iso(59), n=2,
                             total=9, lo=4, hi=5, first_attribute="reused-first", last_attribute="reused-last"),
                        dict(k="other", start=iso(70), end=iso(80), n=1,
                             total=6, lo=6, hi=6, first_attribute="other-reused", last_attribute="other-reused")]
        else:
            # Nonempty arrival-order attributes and two separate intervals for
            # the same opaque key, with a checkpoint containing both intervals.
            events = [(0, "first"), (9, ""), (8, "older-arrival"),
                      (40, "reused-first"), (49, "reused-last"), (48, "")]
            for ordinal, (second, attribute) in enumerate(events):
                emit("reused", second, ordinal, attribute)
            expected = [dict(k="reused", start=iso(0), end=iso(19), n=3,
                             total=6, lo=1, hi=3, first_attribute="first",
                             last_attribute="older-arrival"),
                        dict(k="reused", start=iso(40), end=iso(59), n=3,
                             total=15, lo=4, hi=6, first_attribute="reused-first",
                             last_attribute="reused-last")]
    if collections:
        inputs = [json.loads(line) for line in input_path.read_text().splitlines()]
        for row in expected:
            members = sorted((event for event in inputs if event["k"] == row["k"]
                and row["start"] <= event["timestamp"] < row["end"]),
                key=lambda event: (event["timestamp"], event["ordinal"]))
            row["items"] = [event["metric"] for event in members]
            row["attributes"] = [event["attribute"] for event in members]
            row["selected"] = [event["selected"] for event in members]
    collection_input_schema = ", selected BOOLEAN NOT NULL" if collections else ""
    collection_schema = ", items BIGINT[], attributes TEXT[], selected BOOLEAN[]" if collections else ""
    collection_select = ", items, attributes, selected" if collections else ""
    collection_aggregates = """,
    ARRAY_AGG(metric ORDER BY timestamp, ordinal) AS items,
    ARRAY_AGG(attribute ORDER BY timestamp, ordinal) AS attributes,
    ARRAY_AGG(selected ORDER BY timestamp, ordinal) AS selected""" if collections else ""
    lag = (f"{hot_rows*9+60} seconds" if scenario == "hot" else
           "2 days" if scenario == "continuous" else "1 minute")
    watermark = "timestamp" if scenario == "closed-reuse" else f"timestamp - INTERVAL '{lag}'"
    (directory / "query.sql").write_text(f"""
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL,
  ordinal BIGINT NOT NULL, metric BIGINT NOT NULL, attribute TEXT NOT NULL{collection_input_schema},
  WATERMARK FOR timestamp AS {watermark})
WITH (connector='single_file', path='{{{{INPUT}}}}', format='json',
      type='source', wait_for_control='true');
CREATE TABLE result (k TEXT, start TIMESTAMP, end TIMESTAMP, n BIGINT,
  total BIGINT, lo BIGINT, hi BIGINT, first_attribute TEXT, last_attribute TEXT{collection_schema})
WITH (connector='single_file', path='{{{{OUTPUT}}}}', format='json', type='sink');
INSERT INTO result SELECT k, window.start, window.end, n, total, lo, hi,
  first_attribute, last_attribute{collection_select} FROM (
  SELECT k, SESSION(INTERVAL '10 seconds') AS window, COUNT(*) AS n,
    SUM(metric) AS total, MIN(metric) AS lo, MAX(metric) AS hi,
    FIRST_VALUE(attribute ORDER BY ordinal) FILTER (WHERE attribute <> '') AS first_attribute,
    LAST_VALUE(attribute ORDER BY ordinal) FILTER (WHERE attribute <> '') AS last_attribute{collection_aggregates}
  FROM events GROUP BY k, window);
""")
    (directory / "expected.json").write_text(json.dumps(expected, indent=2)+"\n")
    (directory / "expected.checkpoint.json").write_text(
        json.dumps(expected[:1] if scenario == "closed-reuse" else [], indent=2)+"\n")
    checkpoint = total_rows-1 if scenario in ("many", "hot") else (total_rows//2 if scenario == "continuous" else 4)
    retained_projection = None
    if scenario in ("many", "hot"):
        # The retained-row verifier reads the actual checkpoint IPC values;
        # transport size or source-file length cannot qualify this premise.
        retained_projection = "retained-projection.json"
        projection = dict(fields=[dict(name=name, type=kind, nullable=False)
            for name, kind in (("k", "utf8"), ("ordinal", "int64"),
                               ("metric", "int64"), ("attribute", "utf8"))],
            identity_columns=["k", "ordinal"], utf8_payload_columns=["attribute"],
            expected_rows=checkpoint, minimum_utf8_bytes=checkpoint * payload_bytes)
        (directory / retained_projection).write_text(json.dumps(projection, indent=2)+"\n")
        with input_path.open() as source, (directory / "expected.retained.jsonl").open("w") as oracle:
            for line in itertools.islice(source, checkpoint):
                row = json.loads(line)
                oracle.write(json.dumps({field["name"]: row[field["name"]]
                    for field in projection["fields"]})+"\n")
    manifest = dict(query="query.sql", input="input.jsonl", expected="expected.json",
                    expected_checkpoint="expected.checkpoint.json",
                    checkpoint_input_rows=checkpoint, checkpoint_output_rows=1 if scenario == "closed-reuse" else 0,
                    batch_rows=1 if scenario in ("ordered-reuse", "closed-reuse") else 32,
                    timeout_seconds=900, rss_limit_mib=512,
                    description=scenario,
                    retained_payload_bytes_per_input=payload_bytes if scenario in ("many", "hot") else 0,
                    retained_projection=retained_projection,
                    retained_expected="expected.retained.jsonl" if retained_projection else None,
                    retained_row_key_prefix_hex="52" if retained_projection else None,
                    retained_payload_proof=("Every prefix row has an independently encoded equal-size attribute; "
                        "the SQL final ordered attribute expressions retain raw input; "
                        "watermark lags every input timestamp, and all prefix sessions are open."
                        if scenario in ("many", "hot") else ""))
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest,indent=2)+"\n")
    print(f"PREPARED {path}: input={total_rows}, checkpoint={checkpoint}, outputs={len(expected)}",flush=True)
    return path


def run(manifest_path, binary, root, backend, protocol, batch=None):
    manifest = json.loads(manifest_path.read_text())
    base = manifest_path.parent
    def source(name):
        return (base / manifest[name]).resolve(strict=True)
    expected = ArrayRows(source("expected"))
    checkpoint_expected = ArrayRows(source("expected_checkpoint"))
    prefix = manifest["checkpoint_input_rows"]
    checkpoint_outputs = manifest["checkpoint_output_rows"]
    input_path = source("input")
    with input_path.open() as stream:
        input_rows = sum(1 for _ in stream)
    if type(prefix) is not int or not 0 < prefix < input_rows:
        raise ValueError("checkpoint input must be a positive proper prefix")
    if type(checkpoint_outputs) is not int or not 0 <= checkpoint_outputs <= len(expected):
        raise ValueError("invalid checkpoint output count")
    if len(checkpoint_expected) != checkpoint_outputs:
        raise ValueError("checkpoint cardinality and complete expected rows disagree")
    payload_floor = manifest.get("retained_payload_bytes_per_input", 0)
    proof = manifest.get("retained_payload_proof", "")
    if type(payload_floor) is not int or payload_floor < 0:
        raise ValueError("retained payload floor must be a nonnegative integer")
    if payload_floor and (not isinstance(proof, str) or not proof.strip()):
        raise ValueError("positive retained floor requires declared retained_payload_proof")
    batch = manifest.get("batch_rows", 1) if batch is None else batch
    directory = root / f"{backend}-{batch}-{protocol}"
    directory.mkdir(parents=True,exist_ok=True)
    output = directory / "output.jsonl"
    def sql_path(path):
        return str(path).replace("'", "''")
    query = source("query").read_text().replace("{{INPUT}}",sql_path(input_path)).replace("{{OUTPUT}}",sql_path(output))
    query_path = directory / "query.sql"
    query_path.write_text(query)
    env = dict(os.environ)
    # Make caller environment unable to silently weaken capture minima or
    # enable another route. Optional idle observations are declared below.
    for name in list(env):
        if name.startswith("STREAMR_CAPTURE_") or name.startswith("STREAMR_TEST_"):
            env.pop(name)
    timeout = manifest.get("timeout_seconds",900)
    env.update(STREAMR_TEST_NATIVE_WINDOWS="1",STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_EXECUTION_BYTES=str(16*1024*1024),
        STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(timeout),
        STREAMR_CAPTURE_QUERY=str(query_path),STREAMR_CAPTURE_OUTPUT=str(output),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(prefix),
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(len(expected)),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(checkpoint_outputs),
        STREAMR_CAPTURE_EXPECTED_ROWS=str(len(expected)),STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
    idle = manifest.get("idle")
    if idle:
        env.update(STREAMR_CAPTURE_IDLE_SOURCE_ROW_TARGET=str(idle["source_row_target"]),
            STREAMR_CAPTURE_IDLE_SECONDS=str(idle["seconds"]),
            STREAMR_CAPTURE_IDLE_MIN_PRE_ROWS=str(len(idle["expected_before"])),
            STREAMR_CAPTURE_IDLE_MAX_BYTES=str(idle["max_output_bytes"]))
        if "pre_match_pointer" in idle:
            env["STREAMR_CAPTURE_IDLE_PRE_MATCH_POINTER"] = idle["pre_match_pointer"]
            env["STREAMR_CAPTURE_IDLE_PRE_MATCH_VALUE"] = json.dumps(idle["pre_match_value"])
    log = directory / "capture.log"
    status,rss = capacity.child_with_usage([str(binary),"external_sql_checkpoint_capture",
        "--ignored","--test-threads=1","--nocapture"],env,log,timeout)
    if status or "1 passed" not in log.read_text():
        raise RuntimeError(f"capture failed ({status}): {log}")
    exact_rows(output.with_suffix(".initial.jsonl"),expected)
    exact_rows(output,expected)
    committed = re.findall(r"^CAPTURE_RESULT phase=recovered checkpoint=\d+ "
        r"input_rows_before_checkpoint=\d+ committed_rows=(\d+) ", log.read_text(), re.MULTILINE)
    if len(committed) != 1 or int(committed[0]) != checkpoint_outputs:
        raise AssertionError(f"missing/inconsistent committed prefix metadata: {log}")
    exact_rows(output, checkpoint_expected, prefix_rows=int(committed[0]))
    if idle:
        for phase in ("initial","recovered"):
            for boundary in ("before","after"):
                exact_rows(output.with_suffix(f".idle-{phase}-{boundary}.jsonl"),idle[f"expected_{boundary}"])
    if rss > manifest.get("rss_limit_mib",512)*1024*1024:
        raise RuntimeError(f"capture RSS {rss} exceeds declared limit")
    floor = prefix * payload_floor
    result = dict(backend=backend,protocol=protocol,initial_rows=len(expected),
                  recovered_rows=len(expected),checkpoint_input_rows=prefix,
                  checkpoint_output_rows=checkpoint_outputs,peak_rss_bytes=rss,
                  declared_pool_budget_mib=capacity.POOL_BUDGET_MIB,
                  declared_checkpoint_retained_payload_floor_bytes=floor,
                  measured_checkpoint_retained_payload_bytes=None,
                  retained_checkpoint_measurement_required=bool(payload_floor),
                  retained_checkpoint_verifier="scripts/verify-retained-checkpoint-rows.py",
                  retained_payload_proof=proof, checkpoint_prefix_values_verified=True,
                  checkpoint_state_exceeds_10x_pool_budget=None,
                  declared_payload_exceeds_10x_pool_budget=floor>=10*capacity.POOL_BUDGET_MIB*1024*1024)
    (directory/"measurement.json").write_text(json.dumps(result,indent=2)+"\n")
    print(f"PASS {directory}: exact initial/recovered rows; RSS={rss}",flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    choice=parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--manifest",type=Path,help="Caller-owned manifest; see session qualification documentation")
    choice.add_argument("--prepare",choices=("many","hot","continuous","ordered-reuse","closed-reuse"))
    parser.add_argument("--keys",type=int,default=65000)
    parser.add_argument("--payload-bytes",type=int,default=4096)
    parser.add_argument("--rows",type=int,default=65001,help="Hot-session input rows; checkpoint retains rows minus one")
    parser.add_argument("--binary",type=Path)
    parser.add_argument("--batch-rows",type=int,action="append",choices=(1,8,32),
                        help="Override source batches; repeat to run a batch matrix")
    parser.add_argument("--backend",choices=("memory","rocksdb"),action="append")
    parser.add_argument("--protocol",choices=("controller","leader"),action="append")
    args=parser.parse_args()
    if args.keys<1 or args.payload_bytes<1 or args.rows<2:
        parser.error("keys/payload bytes must be positive and hot rows at least two")
    root=args.directory.resolve()
    path=(prepare(root,args.prepare,args.keys,args.payload_bytes,args.rows) if args.prepare else args.manifest.resolve(strict=True))
    if args.binary:
        manifest = json.loads(path.read_text())
        binary=args.binary.resolve(strict=True)
        default_backends = ("memory", "rocksdb") if manifest.get("description") in ("ordered-reuse", "closed-reuse") else ("rocksdb",)
        for backend in dict.fromkeys(args.backend or default_backends):
            for protocol in dict.fromkeys(args.protocol or ("controller","leader")):
                batches = args.batch_rows or ((1, 8) if manifest.get("description") == "ordered-reuse"
                                               else (manifest.get("batch_rows", 1),))
                for batch in dict.fromkeys(batches):
                    run(path,binary,root,backend,protocol,batch)


if __name__=="__main__":
    main()
