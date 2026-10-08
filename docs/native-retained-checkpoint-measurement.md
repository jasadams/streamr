# Native retained-checkpoint measurement

These read-only tools inspect engine-produced, locally retained format-2
Parquet checkpoints. They measure retained artifacts independently of SQL output
oracles; they do not run SQL, recover workers or establish live memory bounds.
Use trusted artifacts from a pinned engine build. Post-decode size checks are
not a safety boundary for arbitrary untrusted Parquet or Arrow files.

`scripts/native-checkpoint-inventory.py` selects the checkpoint identified by a
capture log and explicit epoch, operator and table. It validates controller or
leader metadata, singleton partition-local ownership, schema identity, exact
file inventory, bounded object sizes and SHA-256. Its row counts are metadata
declarations; it does not decode values or measure logical payload.

Supply generated `api_pb2.py` and `rpc_pb2.py` bindings from the matching engine
source, with the Python protobuf runtime available. For example, generate the
bindings in the required development container using its existing `protoc`:

```sh
mkdir -p target/checkpoint-python-bindings
podman exec -w /app streamr-state-build protoc \
  -I crates/arroyo-rpc/proto --python_out=target/checkpoint-python-bindings \
  crates/arroyo-rpc/proto/api.proto crates/arroyo-rpc/proto/rpc.proto
python3 scripts/native-checkpoint-inventory.py \
  --capture-log CASE/capture.log --storage-root CHECKPOINT_ROOT \
  --bindings target/checkpoint-python-bindings \
  --operator OPERATOR --table TABLE --epoch 1 --output CASE/inventory.json
```

Retain checkpoints in a mounted results directory with the existing
`ARROYO__CHECKPOINT_URL` configuration during capture. A disposable container's
default temporary directory disappears when the container is removed.

`scripts/verify-retained-checkpoint-rows.py` consumes that passed inventory and
checks exact caller-projected raw Arrow IPC rows inside the actual Parquet
objects. It requires pinned PyArrow 19.0.1 in a compatible Python environment.
This dependency is qualification tooling, not an engine dependency. Supply the
opaque logical row-key prefix, declared field types/nullability/timezone, unique
identity columns, UTF8 payload columns to count and an independently prepared
expected JSONL file. Every expected row must contain exactly the declared fields.
Supported types are UTF8, large UTF8, boolean, signed/unsigned integers and
timestamps represented as exact integer nanoseconds. Other types are rejected
when projected. Unprojected fields are not value-qualified.

Example caller declaration:

```json
{
  "fields": [
    {"name": "id", "type": "int64", "nullable": false},
    {"name": "payload", "type": "utf8", "nullable": false}
  ],
  "identity_columns": ["id"],
  "utf8_payload_columns": ["payload"],
  "expected_rows": 2,
  "minimum_utf8_bytes": 8
}
```

```sh
python3 scripts/verify-retained-checkpoint-rows.py \
  --inventory CASE/inventory.json \
  --inventory-helper scripts/native-checkpoint-inventory.py \
  --storage-root CHECKPOINT_ROOT --row-key-prefix-hex PREFIX \
  --projection CALLER/projection.json --expected CALLER/expected.jsonl \
  --directory CASE/retained-row-proof
```

Prepare expected values from the original source fixture and declared checkpoint
prefix, never from captured IPC values. Use integer arithmetic for timestamp
nanoseconds. The verifier rejects missing, extra or duplicated identities and
compares every projected value. It counts actual selected UTF8 bytes once per
raw row, excluding schema, key, index, transport and duplicate-copy overhead.
An eight-MiB SQLite cache and disk ledger avoid keeping the entire oracle in a
Python map. Object, row-group, decoded batch and raw IPC limits are explicit;
these checks do not qualify the separate live engine process's RSS.

The result pins inventory, helpers, caller declarations/oracles and PyArrow
extension hashes. Existing result directories are preserved rather than
overwritten. Retain the original capture, build provenance and checkpoint
objects alongside these measurements; a passed measurement alone does not prove
that the live state exceeded its configured budget or recovered correctly.

Both v5 RocksDB SESSION checkpoint protocols passed a small actual-object
measurement in `target/native-m3-retained-ipc-small-v1/`: seven independently
expected projected rows and 448 retained attribute UTF8 bytes. Wrong payload,
missing row, duplicate identity, wrong type and a one-nanosecond timestamp change
were all rejected. This qualifies the measurement path on that small fixture,
not large state or full milestone readiness.
