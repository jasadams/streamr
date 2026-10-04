#!/usr/bin/env python3
"""Synthetic wire and corruption checks for the read-only inventory inspector."""

import importlib.util
from pathlib import Path
import hashlib
import struct
import tempfile

spec = importlib.util.spec_from_file_location("inventory", Path(__file__).with_name("checkpoint_inventory.py"))
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)


def vint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def nfield(number, value):
    return vint(number << 3) + vint(value)


def bfield(number, value):
    return vint((number << 3) | 2) + vint(len(value)) + value


def map_entry(number, key, value):
    keyfield = bfield(1, key) if isinstance(key, bytes) else nfield(1, key)
    return bfield(number, keyfield + bfield(2, value))


def make_case(root, mode, *, state_version=1, padding=0):
    job, operator, epoch, table = "job", "aggregate-1", 1, "native-aggregate-v1"
    base = (f"{job}/checkpoints/checkpoint-{epoch:07}" if mode == "controller" else
            f"pipe-test/{job}/generations/0/checkpoints/checkpoint-{epoch:07}")
    table_bytes = table.encode()
    namespace = b"\x01\x00" + struct.pack(">III", 0, 1, len(table_bytes)) + table_bytes
    logical_key = b"G" + struct.pack(">I", 1) + b"x"
    key = namespace + logical_key.replace(b"\x00", b"\x00\xff") + b"\x00\x00"
    value = b"STRAGG02" + struct.pack(">qqqII", 11, 0, 1, 1, 1) + b"ipc"
    page = b"STRDS001" + struct.pack(">II", len(key), len(value)) + key + value + b"x" * padding
    page_ref = f"{base}/operator-{operator}/table-{table}-000/disk-fixture-000000.bin"
    filemeta = (bfield(1, page_ref.encode()) + nfield(2, len(page))
                + bfield(3, hashlib.sha256(page).digest()) + nfield(4, 1))
    subtask = (nfield(2, 1) + nfield(3, 1)
               + bfield(4, b"schema") + bfield(5, namespace)
               + nfield(7, 1)
               + bfield(9, filemeta))
    task = nfield(1, 1) + bfield(2, bfield(2, subtask))
    disk_config = bfield(1, table_bytes) + nfield(2, 1) + bfield(3, b"schema")
    config = nfield(1, 3) + bfield(2, disk_config) + nfield(3, state_version)
    table_cp = nfield(1, 3) + bfield(2, task)
    identity = (bfield(1, job.encode()) + bfield(2, operator.encode())
                + nfield(3, epoch) + nfield(6, 1))
    op = (bfield(1, identity) + map_entry(13, table_bytes, table_cp)
          + map_entry(14, table_bytes, config))
    if mode == "controller":
        cp = bfield(1, job.encode()) + nfield(2, epoch) + bfield(6, operator.encode())
        write(root, f"{base}/metadata", cp)
        write(root, f"{base}/operator-{operator}/metadata", op)
    else:
        cp = (bfield(1, b"pipe-test") + bfield(2, job.encode())
              + nfield(4, epoch) + bfield(10, op))
        write(root, f"{base}/checkpoint-manifest.pb", cp)
    write(root, page_ref, page)
    return page_ref


def write(root, relative, value):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    for mode in ("controller", "leader"):
        case = root / mode
        case.mkdir()
        page_ref = make_case(case, mode)
        result = inventory.inspect(case, "job", 1, mode, "pipe-test", 0,
                                   "native-aggregate-v1", 1)
        assert result["aggregate_owners"][0]["prefix_counts"]["G"] == 1
        assert result["aggregate_owners"][0]["group_header_bounds"]["next_ordinal"]["max"] == 1
        page = case / page_ref
        data = page.read_bytes()
        page.write_bytes(data[:-1] + b"!")
        try:
            inventory.inspect(case, "job", 1, mode, "pipe-test", 0,
                              "native-aggregate-v1", 1)
        except inventory.Invalid as error:
            assert "checksum" in str(error)
        else:
            raise AssertionError("corrupt checkpoint page was accepted")
    swapped = root / "swapped-controller"
    swapped.mkdir()
    make_case(swapped, "controller")
    operator_metadata = (swapped / "job/checkpoints/checkpoint-0000001/"
                         "operator-aggregate-1/metadata")
    operator_metadata.write_bytes(operator_metadata.read_bytes().replace(
        b"aggregate-1", b"aggregate-2"))
    try:
        inventory.inspect(swapped, "job", 1, "controller", "pipe-test", 0,
                          "native-aggregate-v1", 1)
    except inventory.Invalid as error:
        assert "operator metadata differs" in str(error)
    else:
        raise AssertionError("swapped controller operator metadata was accepted")
    version = root / "unsupported-version"
    version.mkdir()
    make_case(version, "leader", state_version=2)
    try:
        inventory.inspect(version, "job", 1, "leader", "pipe-test", 0,
                          "native-aggregate-v1", 1)
    except inventory.Invalid as error:
        assert "state version" in str(error)
    else:
        raise AssertionError("unsupported table state version was accepted")
    oversized = root / "oversized"
    oversized.mkdir()
    make_case(oversized, "leader", padding=inventory.MAX_PAGE_SIZE)
    try:
        inventory.inspect(oversized, "job", 1, "leader", "pipe-test", 0,
                          "native-aggregate-v1", 1)
    except inventory.Invalid as error:
        assert "restore size limit" in str(error)
    else:
        raise AssertionError("oversized checkpoint page was accepted")
    assert inventory.key_kind(b"G\0\0\0\x01x") == "G"
    for malformed in (b"Z", b"G\0\0\0\x02x", b"R\0\0\0\0"):
        try:
            inventory.key_kind(malformed)
        except inventory.Invalid:
            pass
        else:
            raise AssertionError(f"malformed key accepted: {malformed!r}")
print("PASS synthetic inventories, selected identities, version, page bounds and corruption")
