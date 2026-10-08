#!/usr/bin/env python3
"""Capture and compare caller-declared typed CDC result composition.

No expected values are derived from captured output. The manifest supplies
complete prefix states, checkpoint/final rows, keys and intermediate values.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

_spec = importlib.util.spec_from_file_location(
    "window_capacity", Path(__file__).with_name("test-native-window-capacity.py"))
capacity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capacity)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_object)


class Oracle:
    def __init__(self, expected):
        self.keys = expected["key_fields"]
        if not self.keys or len(set(self.keys)) != len(self.keys):
            raise ValueError("key_fields must be a nonempty unique list")
        self.allowed = {}
        self.fields = None
        for state in expected["prefix_states"]:
            for key, row in self.index(state).items():
                values = self.allowed.setdefault(key, {field: set() for field in row})
                for field, value in row.items():
                    values[field].add(canonical(value))
        if not self.allowed:
            raise ValueError("at least one declared prefix row is required")
        for field, values in expected.get("intermediate_values", {}).items():
            if field in self.keys or field not in self.fields:
                raise ValueError("intermediate values cannot change keys or add fields")
            if type(values) is not list:
                raise ValueError("intermediate alternatives must be arrays of typed JSON values")
            for allowed in self.allowed.values():
                allowed[field].update(canonical(value) for value in values)
        self.checkpoint = self.index(expected["checkpoint"])
        self.final = self.index(expected["final"])
        for row in (*self.checkpoint.values(), *self.final.values()):
            self.check(row)

    def key(self, row):
        if type(row) is not dict or not all(field in row for field in self.keys):
            raise ValueError("oracle/capture row has missing keys")
        return canonical([row[field] for field in self.keys])

    def index(self, rows):
        if type(rows) is not list:
            raise ValueError("expected states must be arrays of complete rows")
        result = {}
        for row in rows:
            key = self.key(row)
            if self.fields is None:
                self.fields = set(row)
            if set(row) != self.fields or key in result:
                raise ValueError("oracle row fields differ or key repeats")
            result[key] = row
        return result

    def check(self, row):
        key = self.key(row)
        if set(row) != self.fields or key not in self.allowed:
            raise ValueError("capture row fields or key differ from oracle")
        for field, value in row.items():
            if canonical(value) not in self.allowed[key][field]:
                raise ValueError(f"{key}: {field} is not a typed declared branch-prefix value")
        return key

    def reduce(self, path):
        current, snapshots = {}, []
        with path.open() as stream:
            for line in stream:
                if not line.endswith("\n") or not line.strip():
                    raise ValueError(f"{path}: incomplete/blank JSONL")
                payload = json.loads(line, object_pairs_hook=unique_object)
                if type(payload) is dict and set(payload) == {"payload"}:
                    payload = payload["payload"]
                if type(payload) is not dict or set(payload) != {"before", "after", "op"}:
                    raise ValueError(f"{path}: CDC envelope differs")
                before, after, op = payload["before"], payload["after"], payload["op"]
                if (op not in ("c", "u", "d") or (before is None) != (op == "c")
                        or (after is None) != (op == "d")):
                    raise ValueError(f"{path}: CDC operation/images differ")
                key = self.check(after if after is not None else before)
                if before is not None and self.check(before) != key:
                    raise ValueError(f"{path}: before/after key differs")
                if canonical(before) != canonical(current.get(key)):
                    raise ValueError(f"{path}: CDC before-image continuity differs")
                if after is None:
                    del current[key]
                else:
                    current[key] = after
                snapshots.append(dict(current))
        return snapshots


def compare(directory, oracle, prefix, output):
    log = (directory / "capture.log").read_text()
    initial = re.findall(r"^CAPTURE_RESULT phase=initial rows=(\d+) path=(.+)$", log, re.M)
    recovered = re.findall(r"^CAPTURE_RESULT phase=recovered checkpoint=(\d+) "
        r"input_rows_before_checkpoint=(\d+) committed_rows=(\d+) rows=(\d+) "
        r"bytes=(\d+) path=(.+) job=(\S+)$", log, re.M)
    if (len(initial) != 1 or len(recovered) != 1
            or len(re.findall(r"^test result: ok\. 1 passed; 0 failed;", log, re.M)) != 1):
        raise ValueError("capture did not report one complete passing initial/recovery run")
    count, initial_path = initial[0]
    epoch, input_prefix, committed, final_count, _, final_path, _ = recovered[0]
    if (epoch, input_prefix, initial_path, final_path) != (
            "1", str(prefix), str(output.with_suffix(".initial.jsonl")), str(output)):
        raise ValueError("capture epoch, source prefix or output paths differ")
    first = oracle.reduce(output.with_suffix(".initial.jsonl"))
    restored = oracle.reduce(output)
    committed = int(committed)
    if (len(first) != int(count) or len(restored) != int(final_count)
            or not first or not 1 <= committed <= len(restored)):
        raise ValueError("capture cardinality or committed prefix differs")
    if (canonical(first[-1]) != canonical(oracle.final)
            or canonical(restored[committed - 1]) != canonical(oracle.checkpoint)
            or canonical(restored[-1]) != canonical(oracle.final)):
        raise ValueError("initial/checkpoint/fresh-worker complete values differ")
    return dict(initial_rows=len(first), committed_rows=committed, recovered_rows=len(restored))


def run(manifest_path, binary, root, backend, protocol, batch, evidence):
    manifest = read_json(manifest_path)
    paths = {field: (manifest_path.parent / manifest[field]).resolve(strict=True)
             for field in ("query", "input", "expected")}
    expected = read_json(paths["expected"])
    oracle = Oracle(expected)
    prefix = manifest["checkpoint_input_rows"]
    with paths["input"].open() as source:
        rows = sum(1 for _ in source)
    if type(prefix) is not int or not 0 < prefix < rows:
        raise ValueError("checkpoint_input_rows must be a proper positive input prefix")
    directory = root / f"{backend}-{protocol}-batch{batch}"
    directory.mkdir(parents=True, exist_ok=False)
    output = directory / "output.jsonl"
    query = paths["query"].read_text()
    for token, path in (("{{INPUT}}", paths["input"]), ("{{OUTPUT}}", output)):
        if token not in query:
            raise ValueError(f"query lacks {token} connector path placeholder")
        query = query.replace(token, str(path).replace("'", "''"))
    query_path = directory / "query.sql"
    query_path.write_text(query)
    inventory = {field: {"path": str(path), "sha256": digest(path)} for field, path in paths.items()}
    inventory.update(binary={"path": str(binary), "sha256": digest(binary)},
                     manifest={"path": str(manifest_path), "sha256": digest(manifest_path)},
                     rendered_query={"path": str(query_path), "sha256": digest(query_path)},
                     runner={"path": str(Path(__file__).resolve()), "sha256": digest(Path(__file__))})
    if evidence:
        inventory["source_evidence"] = {"path": str(evidence), "sha256": digest(evidence)}
    timeout = manifest.get("timeout_seconds", 90)
    maximum = manifest.get("max_capture_rows", 128)
    if type(timeout) is not int or timeout < 1 or type(maximum) is not int or maximum < 1:
        raise ValueError("timeout_seconds and max_capture_rows must be positive integers")
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("STREAMR_TEST_", "STREAMR_CAPTURE_"))}
    env.update(STREAMR_TEST_NATIVE_AGGREGATES="1", STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_CHECKPOINT_MODE=protocol, STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_AGGREGATE_FLUSH_SECONDS="3600", STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=str(timeout), STREAMR_CAPTURE_QUERY=str(query_path),
        STREAMR_CAPTURE_OUTPUT=str(output), STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(prefix),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1")
    for phase in ("INITIAL", "CHECKPOINT", ""):
        name = f"{phase}_ROWS" if phase else "ROWS"
        env[f"STREAMR_CAPTURE_EXPECTED_{name}"] = "1"
        env[f"STREAMR_CAPTURE_MAX_{name}"] = str(maximum)
    limits = manifest.get("test_limits", {})
    valid = {"MAX_OPEN_DATABASES", "MAX_SNAPSHOTS", "SCAN_PAGE_BYTES", "QUEUED_WRITE_BYTES"}
    if not set(limits) <= valid or any(type(value) is not int or value < 1 for value in limits.values()):
        raise ValueError("test_limits must contain declared positive supported resource overrides")
    env.update({f"STREAMR_TEST_{key}": str(value) for key, value in limits.items()})
    (directory / "inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
    (directory / "capture-env.json").write_text(json.dumps({k: v for k, v in env.items()
        if k.startswith(("STREAMR_TEST_", "STREAMR_CAPTURE_"))}, indent=2) + "\n")
    status, rss = capacity.child_with_usage([str(binary), "external_sql_checkpoint_capture",
        "--ignored", "--test-threads=1", "--nocapture"], env, directory / "capture.log", timeout)
    if status:
        raise RuntimeError(f"capture failed ({status}): {directory / 'capture.log'}")
    if any(digest(Path(value["path"])) != value["sha256"] for value in inventory.values()):
        raise ValueError("source/query/oracle/binary inventory changed during capture")
    result = compare(directory, oracle, prefix, output)
    result.update(status="pass", backend=backend, protocol=protocol, batch_rows=batch,
                  peak_child_rss_bytes=rss, cross_branch_atomicity_qualified=False,
                  source_evidence_supplied=evidence is not None,
                  inventory_sha256=digest(directory / "inventory.json"))
    result["artifacts"] = {p.name: digest(p) for p in (directory / "capture.log",
        output, output.with_suffix(".initial.jsonl"))}
    (directory / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("binary", type=Path)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--source-evidence", type=Path)
    parser.add_argument("--backend", choices=("memory", "rocksdb"), action="append")
    parser.add_argument("--protocol", choices=("controller", "leader"), action="append")
    parser.add_argument("--batch", choices=(1, 8), type=int, action="append")
    args = parser.parse_args()
    evidence = args.source_evidence.resolve(strict=True) if args.source_evidence else None
    results = []
    for backend in dict.fromkeys(args.backend or ("memory", "rocksdb")):
        for protocol in dict.fromkeys(args.protocol or ("controller", "leader")):
            for batch in dict.fromkeys(args.batch or (1, 8)):
                results.append(run(args.manifest.resolve(strict=True), args.binary.resolve(strict=True),
                    args.directory.resolve(), backend, protocol, batch, evidence))
    print(json.dumps({"status": "pass", "cases": results}, indent=2))


if __name__ == "__main__":
    main()
