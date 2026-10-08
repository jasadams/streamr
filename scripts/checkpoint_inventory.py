#!/usr/bin/env python3
"""Read a selected local Streamr checkpoint's native-aggregate logical keys.

Read-only diagnostic. Select the exact job, epoch and checkpoint protocol; do
not discover a candidate by listing directories. Set ARROYO__CHECKPOINT_URL to
file:///app/target/... for the SQL capture so storage survives its container.
"""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import struct
import sys


TABLE_TYPE_DISK = 3
TABLE_NAME = "native-aggregate-v1"
PAGE_MAGIC = b"STRDS001"
GROUP_MAGIC = b"STRAGG02"
KINDS = "GDMREC"
PAGE_BYTES = 1024 * 1024
PAGE_ROWS = 128
MAX_PAGE_SIZE = PAGE_BYTES + PAGE_ROWS * 8 + len(PAGE_MAGIC)


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def varint(data, position):
    value = 0
    start = position
    for shift in range(0, 70, 7):
        require(position < len(data), "truncated protobuf varint")
        byte = data[position]
        position += 1
        value |= (byte & 127) << shift
        if not byte & 128:
            require(value <= 0xffffffffffffffff, "protobuf varint overflow")
            require(position - start == max(1, (value.bit_length() + 6) // 7),
                    "noncanonical protobuf varint")
            return value, position
    raise Invalid("protobuf varint too long")


def message(data, allowed):
    position = 0
    result = defaultdict(list)
    while position < len(data):
        tag, position = varint(data, position)
        number, wire = tag >> 3, tag & 7
        require(number in allowed, f"unsupported protobuf field {number}")
        require(wire == allowed[number], f"field {number} has unexpected wire type")
        if wire == 0:
            value, position = varint(data, position)
        elif wire == 2:
            length, position = varint(data, position)
            require(length <= len(data) - position, "truncated protobuf field")
            value = data[position:position + length]
            position += length
        else:
            raise Invalid(f"unsupported protobuf wire type {wire}")
        result[number].append(value)
    return result


def one(msg, field, *, required=True, default=None):
    found = msg.get(field, [])
    require(len(found) <= 1, f"duplicate scalar protobuf field {field}")
    require(not required or found, f"missing protobuf field {field}")
    return found[0] if found else default


def text(value, context):
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise Invalid(f"invalid UTF-8 {context}") from error


def map_field(msg, number, key_wire, value_wire):
    result = {}
    for encoded in msg.get(number, []):
        entry = message(encoded, {1: key_wire, 2: value_wire})
        # Proto3 omits a default-valued map key; subtask zero is common.
        key = one(entry, 1, required=False, default=0 if key_wire == 0 else b"")
        value = one(entry, 2)
        require(key not in result, f"duplicate protobuf map key in field {number}")
        result[key] = value
    return result


def metadata_path(root, relative):
    require(relative and not relative.startswith("/"), "absolute checkpoint object path")
    parts = relative.split("/")
    require(all(part not in ("", ".", "..") for part in parts),
            "checkpoint object path escapes storage root")
    root = root.resolve(strict=True)
    candidate = root.joinpath(*parts).resolve(strict=True)
    require(candidate.is_relative_to(root), "checkpoint object symlink escapes storage root")
    require(candidate.is_file(), "checkpoint object is not a regular file")
    return candidate


def read(root, relative):
    return metadata_path(root, relative).read_bytes()


def selected_operators(root, job, epoch, mode, pipeline, generation):
    if mode == "controller":
        base = f"{job}/checkpoints/checkpoint-{epoch:07}"
        cp = message(read(root, f"{base}/metadata"),
                     {1: 2, 2: 0, 3: 0, 4: 0, 5: 0, 6: 2})
        require(text(one(cp, 1), "job id") == job and one(cp, 2) == epoch,
                "controller checkpoint identity mismatch")
        ids = [text(value, "operator id") for value in cp.get(6, [])]
        require(ids and len(ids) == len(set(ids)), "empty/duplicate checkpoint operators")
        operators = []
        for operator_id in ids:
            path = f"{base}/operator-{operator_id}/metadata"
            op = message(read(root, path), {1: 2, 2: 0, 3: 0, 13: 2, 14: 2})
            identity = message(one(op, 1), {1: 2, 2: 2, 3: 0, 4: 0, 5: 0, 6: 0})
            require(text(one(identity, 2), "operator id") == operator_id,
                    "controller operator metadata differs from checkpoint operator list")
            operators.append(op)
        return base, operators
    base = f"{pipeline}/{job}/generations/{generation}/checkpoints/checkpoint-{epoch:07}"
    manifest = message(read(root, f"{base}/checkpoint-manifest.pb"),
                       {1: 2, 2: 2, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 10: 2, 11: 2})
    require(text(one(manifest, 1), "pipeline id") == pipeline
            and text(one(manifest, 2), "job id") == job
            and one(manifest, 3, required=False, default=0) == generation
            and one(manifest, 4) == epoch,
            "protocol checkpoint identity mismatch")
    require(manifest.get(10), "checkpoint manifest has no operators")
    return base, [message(encoded, {1: 2, 2: 0, 3: 0, 13: 2, 14: 2})
                  for encoded in manifest[10]]


def partition_namespace(encoded, table, subtask):
    require(len(encoded) >= 14 and encoded[:2] == b"\x01\x00",
            "unsupported aggregate namespace version/ownership")
    index, parallelism, length = struct.unpack_from(">III", encoded, 2)
    require(index == subtask and parallelism == 1,
            "aggregate namespace is not singleton partition-local owner")
    require(length == len(table) and encoded[14:] == table,
            "aggregate checkpoint namespace/table mismatch")


def decode_key(encoded, namespace):
    require(encoded.startswith(namespace), "logical key has foreign namespace")
    suffix = encoded[len(namespace):]
    key = bytearray()
    position = 0
    while True:
        require(position < len(suffix), "unterminated escaped logical key")
        byte = suffix[position]
        position += 1
        if byte:
            key.append(byte)
            continue
        require(position < len(suffix), "truncated escaped logical key")
        escaped = suffix[position]
        position += 1
        if escaped == 0:
            require(position == len(suffix), "trailing bytes after logical key")
            break
        require(escaped == 255, "invalid escaped logical key byte")
        key.append(0)
    return bytes(key)


def key_kind(key):
    require(key, "empty native aggregate key")
    kind = chr(key[0])
    require(kind in KINDS, f"unknown native aggregate key prefix {key[0]:02x}")
    if kind == "E":
        require(len(key) >= 13, "truncated expiry key")
        group_len = int.from_bytes(key[9:13], "big")
        require(len(key) == 13 + group_len, "invalid expiry key length")
        return kind
    require(len(key) >= 5, "truncated aggregate key")
    group_len = int.from_bytes(key[1:5], "big")
    tail = key[5 + group_len:]
    require(len(key) >= 5 + group_len, "truncated group bytes")
    if kind in "GD":
        require(not tail, "group/dirty key has trailing bytes")
    elif kind == "C":
        require(len(tail) == 8, "invalid cleanup key length")
    elif kind == "M":
        require(len(tail) >= 20, "truncated member primary key")
    else:
        require(len(tail) >= 24, "truncated member secondary key")
        args_len = int.from_bytes(tail[12:16], "big")
        require(len(tail) == 24 + args_len, "invalid member secondary key length")
    return kind


def disk_table(op, job, epoch, base, table, root, generation):
    identity = message(one(op, 1), {1: 2, 2: 2, 3: 0, 4: 0, 5: 0, 6: 0})
    require(text(one(identity, 1), "operator job") == job and one(identity, 3) == epoch,
            "operator checkpoint identity mismatch")
    operator = text(one(identity, 2), "operator id")
    require(one(identity, 6) == 1, "native aggregate owner is not singleton")
    configs = map_field(op, 14, 2, 2)
    checkpoints = map_field(op, 13, 2, 2)
    table_bytes = table.encode("utf-8")
    present = table_bytes in configs or table_bytes in checkpoints
    if not present:
        return None
    require(table_bytes in configs and table_bytes in checkpoints,
            "native aggregate config/checkpoint missing")
    config_wrapper = message(configs[table_bytes], {1: 0, 2: 2, 3: 0})
    cp_wrapper = message(checkpoints[table_bytes], {1: 0, 2: 2})
    require(one(config_wrapper, 1) == one(cp_wrapper, 1) == TABLE_TYPE_DISK,
            "native aggregate checkpoint has unsupported table type")
    require(one(config_wrapper, 3) == 1,
            "native aggregate table state version is unsupported")
    config = message(one(config_wrapper, 2), {1: 2, 2: 0, 3: 2})
    require(text(one(config, 1), "table name") == table and one(config, 2) == 1,
            "native aggregate table config/version mismatch")
    schema_id = one(config, 3)
    task = message(one(cp_wrapper, 2), {1: 0, 2: 2})
    require(one(task, 1) == 1, "unsupported aggregate checkpoint task format")
    subtasks = map_field(task, 2, 0, 2)
    require(set(subtasks) == {0}, "aggregate checkpoint must have only subtask zero")
    subtask = message(subtasks[0],
                      {1: 0, 2: 0, 3: 0, 4: 2, 5: 2, 6: 0, 7: 0, 8: 0, 9: 2})
    require(one(subtask, 1, required=False, default=0) == 0 and one(subtask, 2) == 1
            and one(subtask, 3) == 1
            and one(subtask, 6, required=False, default=0) == generation
            and one(subtask, 7) == epoch and one(subtask, 4) == schema_id,
            "aggregate subtask format/identity mismatch")
    namespace = one(subtask, 5)
    partition_namespace(namespace, table_bytes, 0)
    files = subtask.get(9, [])
    require(bool(files) != bool(one(subtask, 8, required=False, default=0)),
            "invalid aggregate empty marker")
    counts = Counter()
    value_lengths = {}
    group_header_bounds = {}
    total_rows = 0
    prior_key = None
    expected_prefix = f"{base}/operator-{operator}/table-{table}-000/disk-"
    paths = set()
    for encoded_file in files:
        meta = message(encoded_file, {1: 2, 2: 0, 3: 2, 4: 0})
        path = text(one(meta, 1), "checkpoint page path")
        require(path.startswith(expected_prefix) and path.endswith(".bin")
                and "/" not in path[len(expected_prefix):],
                "checkpoint page belongs to another selected owner/epoch")
        require(path not in paths, "duplicate checkpoint page")
        paths.add(path)
        declared_size = one(meta, 2)
        declared_rows = one(meta, 4)
        require(len(PAGE_MAGIC) < declared_size <= MAX_PAGE_SIZE,
                "checkpoint page exceeds restore size limit")
        require(0 < declared_rows <= PAGE_ROWS,
                "checkpoint page exceeds restore row limit")
        checksum = one(meta, 3)
        require(len(checksum) == 32, "invalid checkpoint SHA-256 length")
        page_path = metadata_path(root, path)
        require(page_path.stat().st_size == declared_size,
                "checkpoint object size mismatch")
        data = page_path.read_bytes()
        require(len(data) == declared_size and hashlib.sha256(data).digest() == checksum,
                "checkpoint page size/checksum mismatch")
        require(data.startswith(PAGE_MAGIC), "unknown checkpoint page format")
        cursor = len(PAGE_MAGIC)
        page_rows = 0
        while cursor < len(data):
            require(len(data) - cursor >= 8, "truncated checkpoint frame header")
            key_len, value_len = struct.unpack_from(">II", data, cursor)
            cursor += 8
            require(key_len > 0 and key_len + value_len <= len(data) - cursor,
                    "truncated checkpoint frame")
            encoded_key = data[cursor:cursor + key_len]
            cursor += key_len
            value = data[cursor:cursor + value_len]
            cursor += value_len
            require(prior_key is None or prior_key < encoded_key,
                    "checkpoint keys are duplicate or out of order")
            prior_key = encoded_key
            logical_key = decode_key(encoded_key, namespace)
            kind = key_kind(logical_key)
            counts[kind] += 1
            size = len(value)
            stats = value_lengths.setdefault(kind, {"count": 0, "min": size,
                                                     "max": size, "sum": 0})
            stats["count"] += 1
            stats["min"] = min(stats["min"], size)
            stats["max"] = max(stats["max"], size)
            stats["sum"] += size
            if kind == "G":
                require(len(value) >= 40 and value[:8] == GROUP_MAGIC,
                        "unknown aggregate group value codec")
                header = {
                    "generation": int.from_bytes(value[16:24], "big"),
                    "next_ordinal": int.from_bytes(value[24:32], "big"),
                    "state_fields": int.from_bytes(value[32:36], "big"),
                    "emitted_fields": int.from_bytes(value[36:40], "big"),
                }
                for field, number in header.items():
                    bounds = group_header_bounds.setdefault(field, {"min": number,
                                                                     "max": number})
                    bounds["min"] = min(bounds["min"], number)
                    bounds["max"] = max(bounds["max"], number)
            page_rows += 1
        require(page_rows == declared_rows, "checkpoint page row count mismatch")
        total_rows += page_rows
    require(total_rows == sum(counts.values()), "checkpoint entry accounting mismatch")
    return {
        "operator_id": operator,
        "namespace_hex": namespace.hex(),
        "entries": total_rows,
        "prefix_counts": {kind: counts[kind] for kind in KINDS},
        "value_lengths": dict(sorted(value_lengths.items())),
        "group_header_bounds": group_header_bounds,
        "page_count": len(files),
    }


def inspect(root, job, epoch, mode, pipeline, generation, table, owner_count):
    base, operators = selected_operators(root, job, epoch, mode, pipeline, generation)
    found = []
    operator_ids = set()
    for op in operators:
        result = disk_table(op, job, epoch, base, table, root, generation)
        if result is None:
            continue
        require(result["operator_id"] not in operator_ids, "duplicate aggregate operator")
        operator_ids.add(result["operator_id"])
        found.append(result)
    require(found, "selected checkpoint contains no native aggregate table")
    if owner_count is not None:
        require(len(found) == owner_count,
                f"expected {owner_count} aggregate owners, found {len(found)}")
    return {"job_id": job, "epoch": epoch, "mode": mode,
            "aggregate_owners": sorted(found, key=lambda owner: owner["operator_id"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-root", type=Path, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--mode", choices=("controller", "leader"), required=True)
    parser.add_argument("--pipeline-id", default="pipe-test")
    parser.add_argument("--generation", type=int, default=0)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--expected-owner-count", type=int)
    args = parser.parse_args()
    require(args.epoch >= 0 and args.generation >= 0, "negative epoch/generation")
    result = inspect(args.storage_root, args.job_id, args.epoch, args.mode,
                     args.pipeline_id, args.generation, args.table,
                     args.expected_owner_count)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (Invalid, OSError) as error:
        print(f"checkpoint inventory rejected: {error}", file=sys.stderr)
        sys.exit(1)
