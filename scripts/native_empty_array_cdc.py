#!/usr/bin/env python3
"""Prepare and check the generic empty ARRAY_AGG/COALESCE CDC capture.

This helper never launches Streamr. `--self-test` uses synthetic capture files.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tempfile

SOURCE = [
    {"timestamp": "2023-10-09T17:13:20", "k": "a", "id": 1, "v": None},
    {"timestamp": "2023-10-09T17:13:21", "k": "a", "id": 2, "v": "x"},
]
CHECKPOINT = {"k": "a", "n": 1, "items": [], "records": []}
FINAL = {"k": "a", "n": 2, "items": ["x"], "records": [{"id": 2, "label": "x"}]}
MATRIX = [(backend, mode, batch) for backend in ("memory", "rocksdb")
          for mode in ("controller", "leader") for batch in (1, 8)]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def read_jsonl(path):
    raw = path.read_bytes()
    if not raw or not raw.endswith(b"\n") or b"\n\n" in raw:
        raise ValueError(f"{path}: empty, blank, or incomplete JSONL")
    rows = [json.loads(line, object_pairs_hook=unique_object)
            for line in raw[:-1].split(b"\n")]
    if any(type(row) is not dict for row in rows):
        raise ValueError(f"{path}: expected JSON objects")
    return rows


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))


def query(runtime_case):
    source = str(runtime_case / "input.jsonl").replace("'", "''")
    sink = str(runtime_case / "output.jsonl").replace("'", "''")
    return f"""SET updating_ttl = NULL;
CREATE TABLE empty_input (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, id BIGINT NOT NULL, v TEXT, WATERMARK FOR timestamp AS timestamp)
WITH (connector='single_file', path='{source}', format='json', type='source', wait_for_control='true');
CREATE TABLE empty_output (k TEXT, n BIGINT, items TEXT[], records STRUCT<id BIGINT, label TEXT>[])
WITH (connector='single_file', path='{sink}', format='debezium_json', type='sink');
INSERT INTO empty_output SELECT k, COUNT(*) AS n,
COALESCE(ARRAY_AGG(v ORDER BY id) FILTER (WHERE v IS NOT NULL), CAST(ARRAY[] AS TEXT[])) AS items,
COALESCE(ARRAY_AGG(named_struct('id', id, 'label', v) ORDER BY id) FILTER (WHERE v IS NOT NULL), CAST(ARRAY[] AS STRUCT<id BIGINT, label TEXT>[])) AS records
FROM empty_input GROUP BY k;
"""


def prepare(host, runtime):
    host = host.resolve()
    if not runtime.is_absolute():
        raise ValueError("runtime root must be an absolute path in the test container")
    host.mkdir(parents=True, exist_ok=True)
    expected_path = host / "expected.json"
    expected_path.write_text(json.dumps({"checkpoint": CHECKPOINT, "final": FINAL}, indent=2) + "\n")
    manifest = {"scope": "existing SQL empty ARRAY_AGG COALESCE CDC checkpoint",
                "source_rows": 2, "checkpoint_prefix": 1,
                "runtime_root": str(runtime),
                "expected_sha256": digest(expected_path), "cases": {}}
    for backend, mode, batch in MATRIX:
        name = f"{backend}-{mode}-batch{batch}"
        directory = host / name
        runtime_case = runtime / name
        directory.mkdir(exist_ok=True)
        for artifact in ("output.jsonl", "output.initial.jsonl", "capture.log"):
            (directory / artifact).unlink(missing_ok=True)
        write_jsonl(directory / "input.jsonl", SOURCE)
        (directory / "query.sql").write_text(query(runtime_case))
        env = {
            "STREAMR_TEST_BACKEND": backend,
            "STREAMR_TEST_CHECKPOINT_MODE": mode,
            "STREAMR_TEST_SOURCE_BATCH_ROWS": str(batch),
            "STREAMR_TEST_NATIVE_AGGREGATES": "1",
            "STREAMR_TEST_AGGREGATE_FLUSH_SECONDS": "3600",
            "STREAMR_CAPTURE_QUERY": str(runtime_case / "query.sql"),
            "STREAMR_CAPTURE_OUTPUT": str(runtime_case / "output.jsonl"),
            "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT": "1",
            "STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS": "1",
            "STREAMR_CAPTURE_MAX_INITIAL_ROWS": "2",
            "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS": "1",
            "STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS": "1",
            "STREAMR_CAPTURE_EXPECTED_ROWS": "2",
            "STREAMR_CAPTURE_MAX_ROWS": "2",
            "STREAMR_CAPTURE_CHECKPOINT_EPOCH": "1",
        }
        manifest["cases"][name] = {
            "backend": backend, "mode": mode, "batch": batch,
            "input_sha256": digest(directory / "input.jsonl"),
            "query_sha256": digest(directory / "query.sql"), "env": env,
        }
    (host / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return {"status": "prepared_not_executed", "cases": len(MATRIX), "prefix": 1}


def strict_row(row):
    if type(row) is not dict or set(row) != {"k", "n", "items", "records"} or (
        type(row["k"]) is not str or type(row["n"]) is not int
        or type(row["items"]) is not list or type(row["records"]) is not list
        or any(type(item) is not str for item in row["items"])
    ):
        raise ValueError(f"wrong typed output row: {row!r}")
    for member in row["records"]:
        if type(member) is not dict or set(member) != {"id", "label"} or (
            type(member["id"]) is not int or type(member["label"]) is not str
        ):
            raise ValueError(f"wrong typed STRUCT member: {member!r}")
    if row not in (CHECKPOINT, FINAL):
        raise ValueError(f"row differs from source-derived states: {row!r}")
    return row


def reduce_cdc(path):
    current = None
    snapshots = []
    for envelope in read_jsonl(path):
        payload = envelope["payload"] if set(envelope) == {"payload"} else envelope
        if type(payload) is not dict or set(payload) != {"before", "after", "op"}:
            raise ValueError(f"{path}: wrong CDC envelope")
        before, after, op = payload["before"], payload["after"], payload["op"]
        if op not in ("c", "u") or (op == "c") != (before is None) or after is None:
            raise ValueError(f"{path}: wrong CDC operation")
        if before is not None:
            strict_row(before)
        strict_row(after)
        if before != current or before == after or (current == FINAL and after == CHECKPOINT):
            raise ValueError(f"{path}: CDC before continuity or source order differs")
        current = after
        snapshots.append(current)
    return snapshots


def compare(host):
    host = host.resolve()
    manifest, expected = read_json(host / "manifest.json"), read_json(host / "expected.json")
    if expected != {"checkpoint": CHECKPOINT, "final": FINAL} or (
        digest(host / "expected.json") != manifest["expected_sha256"]
        or manifest["source_rows"] != 2 or manifest["checkpoint_prefix"] != 1
    ):
        raise ValueError("declared output oracle changed")
    runtime_root = Path(manifest["runtime_root"])
    if not runtime_root.is_absolute() or runtime_root.name != host.name:
        raise ValueError("runtime fixture root differs from prepared host root")
    names = {f"{backend}-{mode}-batch{batch}" for backend, mode, batch in MATRIX}
    if set(manifest["cases"]) != names:
        raise ValueError("case matrix changed")
    results = {}
    for backend, mode, batch in MATRIX:
        name = f"{backend}-{mode}-batch{batch}"
        directory, case = host / name, manifest["cases"][name]
        env = case["env"]
        runtime_case = Path(env["STREAMR_CAPTURE_QUERY"]).parent
        if (case["backend"], case["mode"], case["batch"]) != (backend, mode, batch) or (
            runtime_case != runtime_root / name
            or env != prepare_env(backend, mode, batch, runtime_case)
            or read_jsonl(directory / "input.jsonl") != SOURCE
            or digest(directory / "input.jsonl") != case["input_sha256"]
            or (directory / "query.sql").read_text() != query(runtime_case)
            or digest(directory / "query.sql") != case["query_sha256"]
        ):
            raise ValueError(f"{name}: prepared query, input, or environment differs")
        log = (directory / "capture.log").read_text()
        initial_marker = re.search(r"CAPTURE_RESULT phase=initial rows=(\d+)\b", log)
        recovered_marker = re.search(
            r"CAPTURE_RESULT phase=recovered\b[^\n]*?\bcommitted_rows=(\d+) rows=(\d+)\b", log)
        if "1 passed" not in log or initial_marker is None or recovered_marker is None:
            raise ValueError(f"{name}: external checkpoint capture did not pass")
        initial = reduce_cdc(directory / "output.initial.jsonl")
        recovered = reduce_cdc(directory / "output.jsonl")
        committed, recovered_count = map(int, recovered_marker.groups())
        if not initial or len(initial) != int(initial_marker.group(1)) or (
            initial[-1] != FINAL or committed != 1 or recovered_count != 2
            or recovered != [CHECKPOINT, FINAL]
        ):
            raise ValueError(f"{name}: initial/checkpoint/recovery output differs")
        results[name] = {"initial_rows": len(initial), "committed_rows": committed,
                         "recovered_rows": len(recovered)}
    return {"status": "pass", "scope": "generic empty ARRAY_AGG COALESCE CDC", "cases": results}


def prepare_env(backend, mode, batch, runtime_case):
    return {
        "STREAMR_TEST_BACKEND": backend,
        "STREAMR_TEST_CHECKPOINT_MODE": mode,
        "STREAMR_TEST_SOURCE_BATCH_ROWS": str(batch),
        "STREAMR_TEST_NATIVE_AGGREGATES": "1",
        "STREAMR_TEST_AGGREGATE_FLUSH_SECONDS": "3600",
        "STREAMR_CAPTURE_QUERY": str(runtime_case / "query.sql"),
        "STREAMR_CAPTURE_OUTPUT": str(runtime_case / "output.jsonl"),
        "STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT": "1",
        "STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS": "1",
        "STREAMR_CAPTURE_MAX_INITIAL_ROWS": "2",
        "STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS": "1",
        "STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS": "1",
        "STREAMR_CAPTURE_EXPECTED_ROWS": "2",
        "STREAMR_CAPTURE_MAX_ROWS": "2",
        "STREAMR_CAPTURE_CHECKPOINT_EPOCH": "1",
    }


def self_test():
    with tempfile.TemporaryDirectory() as temp:
        host = Path(temp) / "fixtures"
        prepare(host, Path("/app/target/fixtures"))
        for case in (host / "manifest.json",):
            assert case.is_file()
        for directory in (path for path in host.iterdir() if path.is_dir()):
            write_jsonl(directory / "output.initial.jsonl", [{"before": None, "after": FINAL, "op": "c"}])
            write_jsonl(directory / "output.jsonl", [
                {"before": None, "after": CHECKPOINT, "op": "c"},
                {"before": CHECKPOINT, "after": FINAL, "op": "u"},
            ])
            (directory / "capture.log").write_text(
                "CAPTURE_RESULT phase=initial rows=1\n"
                "CAPTURE_RESULT phase=recovered committed_rows=1 rows=2\n1 passed\n")
        assert compare(host)["status"] == "pass"
        first = host / "memory-controller-batch1"
        write_jsonl(first / "output.initial.jsonl", [
            {"before": None, "after": {**FINAL, "n": True}, "op": "c"}])
        try:
            compare(host)
            raise AssertionError("bool-as-int should be rejected")
        except ValueError:
            pass
        write_jsonl(first / "output.initial.jsonl", [{"before": None, "after": FINAL, "op": "c"}])
        with (first / "output.jsonl").open("a") as stream:
            stream.write('{"op":"u","op":"u"}\n')
        try:
            compare(host)
            raise AssertionError("duplicate JSON fields should be rejected")
        except ValueError:
            pass
        write_jsonl(first / "output.jsonl", [
            {"before": None, "after": CHECKPOINT, "op": "c"},
            {"before": CHECKPOINT, "after": FINAL, "op": "u"},
        ])
        with (first / "query.sql").open("a") as stream:
            stream.write("-- tampered\n")
        try:
            compare(host)
            raise AssertionError("query tampering should be rejected")
        except ValueError:
            pass
    return {"status": "synthetic_oracle_self_test_pass", "runtime_qualified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", nargs="?", type=Path)
    parser.add_argument("--runtime-directory", type=Path)
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        result = self_test()
    elif args.host is None:
        parser.error("host directory is required")
    elif args.compare:
        result = compare(args.host)
    elif args.runtime_directory is not None:
        result = prepare(args.host, args.runtime_directory)
    else:
        parser.error("--runtime-directory is required to prepare")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
