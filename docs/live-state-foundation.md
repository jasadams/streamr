# Live state foundation

`arroyo-state::live` separates mutable working state from the existing Parquet
checkpoint backend. It supplies owned asynchronous reads, ordered atomic write
batches, stable local snapshots and cursor scans. `MemoryLiveState` is the small
job/reference implementation. `RocksLiveState` stores working state on local disk.
Existing SQL operators continue using their existing tables. Remote export,
committed checkpoint restore, SQL execution and rescaling are subsequent work.

## Operator and worker ownership

Create one RocksDB database per job/operator/subtask/generation/attempt using
`RocksStateConfig`. Fresh opens reject existing directories. Explicit `reopen`
requires a matching versioned ownership marker and an existing RocksDB manifest.
Reopen is for diagnostics/local lifecycle tests; a WAL is not a committed
pipeline checkpoint and must not be used to choose recovered pipeline state.

Use `RocksLiveState::open_worker` to share one process-wide resource pool.
`WorkerStateResources::new` supports isolated tests and harnesses; production
operators must use the worker pool. Worker configuration optionally accepts
`worker.live-state-resources` with every field supplied explicitly:

```toml
[worker.live-state-resources]
block-cache-bytes = 8388608
memtable-bytes = 4194304
queued-write-bytes = 1048576
decoded-value-bytes = 1048576
scan-page-bytes = 1048576
max-blocking-operations = 2
max-snapshots = 2
max-open-databases = 4
disk-reserve-bytes = 1073741824
```

These are test-sized examples, not production defaults. Memtables are charged
against the shared cache; do not add the two capacities as independent memory.
Incompatible initialization of a second worker pool fails. The configuration
adapter registers resource metrics once with the default Prometheus registry.

Namespaces express partition-local ownership including unchanged parallelism,
routed hash ranges, explicit replicas, or connector-specific partitions. Routed
keys supply the existing routing hash; the backend does not compute a new one.
Full logical keys and hashes are persisted, so collisions remain distinct. Keys
and values use independently checked version envelopes.

`LiveTableManager` supplies owned logical handles under one backend and one
snapshot boundary. It is also exported from `tables::table_manager` alongside
the legacy manager. Live snapshots have no remote checkpoint publication API yet.
Do not attach these handles to a pipeline that expects legacy checkpoints to
include their content.

Ordinary table names beginning with `streamr.history.v1` are reserved and rejected
by `LiveTableManager::register`. That prefix identifies the internal primary and
expiry tables used by Arrow histories; allowing an ordinary table to claim it
would break logical-table isolation. The restriction preserves the existing
stored-data format. Previously accepted caller names with this prefix must be
renamed before registration; other table names and existing history encodings
are unchanged.

## Admission and lifecycle

Requests exceeding byte budgets fail; capacity saturation awaits admission.
Worker database startup fails immediately when database slots are exhausted,
because waiting could deadlock operators at the worker readiness barrier.
Diagnostic `open` and `reopen` retain asynchronous database admission.
Use `RocksLiveState::admitted_batch(max_bytes, max_operations)` before assembling
pending writes, then add borrowed keys/values and submit with `write_admitted`.
The builder reserves worker bytes before allocating or copying and retains them
through native completion. Existing owned `write_batch` inputs remain caller
allocations while waiting; they must be separately bounded by their producer.
Native request copies and output container overhead are accounted before their
allocation. Reads pin native values and check size before copying. Writes remain
WAL enabled. Diagnostic `open` and `reopen` synchronize each write to disk.
Disposable worker attempts complete atomic native writes before emitting rows,
but omit per-row WAL fsync. Stable snapshots still capture completed writes;
pipeline durability comes from publishing the full committed checkpoint.
Worker recovery always restores that selected checkpoint into fresh storage,
so a newer local WAL is never accepted as committed pipeline state.
Errors propagate and never trigger a RAM fallback.
Native operations run through bounded blocking admission. Cancellation leaves
permits with executing work until it finishes.

Database and snapshot owners reserve cleanup capacity before creation. A bounded
dedicated thread closes native databases and removes snapshot directories after
the last reader drops. `close` awaits destruction; `close_and_remove` also awaits
attempt deletion and propagates removal errors. Snapshots live in separately
owned sibling directories and survive live-database close/removal. Failed fresh
opens remove only directories they created; failed reopen preserves diagnostics.

The disk reserve checks filesystem headroom before writes and checkpoint capture.
They cannot reserve against unrelated filesystem writers or predict every byte
of compaction amplification. Native disk-full and corruption failures propagate.
Deployment reserve must include live state, compaction and overlapping snapshots.

Resource metrics expose limits, used bytes, pending admission, pinned cache bytes,
open databases, snapshots, blocking/cleanup work, native operation latency and disk
headroom/refusals. Latency timers run inside admitted native closures and survive
cancellation of their async caller. Cache
and write-buffer budgets do not cap process RSS. Input batches, retained owned
results, SQL overlays and operator execution remain caller-owned memory. Later
operator migrations must account for those allocations; this foundation does not
claim a larger-than-RAM pipeline guarantee.

## Arrow histories

`ArrowHistory` atomically maintains primary key/time/sequence and timestamp-first
expiry indexes. Full keys are escaped; signed timestamp suffixes are sortable.
The caller supplies timestamp units and unique sequence numbers per key/time.
Each chunk contains one Arrow IPC batch, preserving schema, nulls and timestamps.
Inputs must have bounded source buffers before IPC serialization. Serialized
chunks, scan pages, row counts and write batches are bounded separately; oversized
single rows fail explicitly. Appends commit a chunk at a time, so cancellation
may leave a committed prefix. Repeating the same identifiers replaces that chunk.

Snapshot scans paginate a single hot key over a half-open timestamp range.
Before copying a lookup key, histories reject it if the minimum encoded matching
record exceeds the configured page-byte limit. This applies on both backends,
with or without attached resource accounting; an oversized missing-key lookup
returns a limit error rather than an empty page. Other lookups retain their
existing bounds and result semantics.
Cursors bind to the snapshot and exact namespace/prefix/range. Expiry removes at
most one page at a time, committing a prefix of complete primary/expiry delete
pairs that fits the write-batch limit. Repeated calls make progress without
breaking either index. Records exactly at the cutoff are retained, matching
legacy table retention. IPC frame/body/row bounds are checked before decoding and compressed
IPC is rejected. With shared resources attached via `with_resources`, decoded
page reservations last until the page is dropped; copying or retaining extracted
batches still requires caller accounting. Routed histories require a future
operator routing adapter and are rejected explicitly.

## Native build and validation

The pinned Rust binding is `rocksdb = 0.24.0` and its locked native dependency is
RocksDB 10.4.2. The build uses bundled RocksDB and LZ4, C++ tooling, CMake and
runtime bindgen/libclang. The dev and distribution Dockerfiles and Linux CI
install libclang explicitly. Linux x86_64/arm64 and macOS runners build native
sources for their own architecture; cross-platform artifacts must pass those CI
jobs before distribution. No binary release or license change is part of this work.

Run checks in the Bookworm development environment from `.claude/build-test.md`:

```sh
podman build -f Dockerfile.dev -t arroyo-dev .
podman run --rm -v "$PWD:/app:z" arroyo-dev cargo check --locked -p arroyo-state
podman run --rm -v "$PWD:/app:z" arroyo-dev cargo test --locked -p arroyo-state
podman run --rm -v "$PWD:/app:z" arroyo-dev cargo clippy --locked -p arroyo-state -p arroyo-rpc --all-targets --no-deps -- -D warnings
podman run --rm -v "$PWD:/app:z" arroyo-dev cargo fmt --all -- --check
```

The native probe uses four independent operator databases sharing fixed worker
budgets, bounded random-payload writes, explicit
close/reopen and paginated snapshot verification. It prints elapsed time and
Linux process RSS and peak RSS while cardinality rises. The second argument is
a declared process envelope in MiB; exceeding it fails the probe:

```sh
podman run --rm -v "$PWD:/app:z" arroyo-dev cargo run --locked -p arroyo-state --example live_state_probe -- 256 192
```

This probes native working state, not SQL execution or remote checkpoint recovery.
The probe budgets 8 MiB for cache including memtables, plus 1 MiB each for queued
writes, decoded values and scan work. Native request accounting reserves transient
copies and container overhead in addition to the requested logical page bytes.

## Local qualification (2026-10-03)

Validated in the prescribed Debian Bookworm container on Linux x86_64:

- `cargo check --locked -j4 -p arroyo-state -p arroyo-rpc --all-targets` passed.
- `cargo test --locked -j4 -p arroyo-state -p arroyo-rpc` passed: 29 state tests
  and 32 RPC tests. One existing RPC documentation test is ignored.
- `cargo clippy --locked -j4 -p arroyo-state -p arroyo-rpc --all-targets --no-deps -- -D warnings` passed.
- `cargo fmt -p arroyo-state -p arroyo-rpc -- --check` and `git diff --check` passed.
- The updated development image built successfully with explicit libclang tooling.

The tests include native backend conformance, identity-checked reopen, independent
operators, snapshot lifetime, corruption and cleanup failures, cancellation and
admission, Arrow fidelity, hot-key pagination and atomic expiry indexes.

Supported Linux arm64/macOS distribution builds and complete packaging remain
qualification gates; no local state/RPC test result implies they have passed.

The final native probe (`-- 256 192`) wrote and reopened four operator databases,
then checked every key and all 4096 payload bytes for 65,536 records. Logical
state was 268,435,456 bytes (256 MiB), over 23 times the 11 MiB assigned budgets.
Linux `/proc/self/status` reported 16,084 KiB RSS at baseline, 28,440 KiB after
writes and 34,948 KiB peak after snapshot scans (34.1 MiB), below the declared
192 MiB process envelope. The run took 38.864 seconds. These are observations
from a development build on this host, not production sizing or pipeline limits.
