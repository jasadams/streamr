# Native live-state operations and source-free qualification

This inventory covers configured native state tables/MERGE, updating aggregates,
TUMBLE/HOP and SESSION at the supported fixed ownership. It records current
construction and measurement contracts; it is not a production-default sizing
recommendation, release or application lifecycle parity claim. Consult the exact
capability and validation records before selecting a query.

## Production configuration

The top-level binary accepts `--config PATH` and `--config-dir DIRECTORY`.
Configuration files use kebab-case TOML/YAML keys. `ARROYO__` environment entries
have highest precedence: double underscores become path separators and single
underscores become hyphens. For example:

| YAML/TOML key | Environment variable | Meaning |
| --- | --- | --- |
| `worker.sql-state-backend` | `ARROYO__WORKER__SQL_STATE_BACKEND` | `memory` or `rocksdb`; selection occurs in the construction adapter |
| `worker.disk-sql-state.directory` | `ARROYO__WORKER__DISK_SQL_STATE__DIRECTORY` | Local live RocksDB scratch root |
| `worker.disk-sql-state.max-row-bytes` | `ARROYO__WORKER__DISK_SQL_STATE__MAX_ROW_BYTES` | Disk SQL row/adapter admission limit |
| `worker.execution-resources.memory-bytes` | `ARROYO__WORKER__EXECUTION_RESOURCES__MEMORY_BYTES` | Shared cooperative DataFusion/graph/network pool |
| `worker.execution-resources.max-batch-bytes` | `ARROYO__WORKER__EXECUTION_RESOURCES__MAX_BATCH_BYTES` | Supported emitted/input batch allowance |
| `worker.live-state-resources.disk-reserve-bytes` | `ARROYO__WORKER__LIVE_STATE_RESOURCES__DISK_RESERVE_BYTES` | Free filesystem headroom required at admission |
| `checkpoint-url` | `ARROYO__CHECKPOINT_URL` | Existing remote/filesystem checkpoint store, separate from local scratch |
| `job-controller` | `ARROYO__JOB_CONTROLLER` | Existing `controller` or `worker` mode; test harness calls worker mode `leader` |
| `admin.http-port` | `ARROYO__ADMIN__HTTP_PORT` | Admin HTTP endpoint containing `/metrics` |

`worker.live-state-resources` requires explicit `block-cache-bytes`,
`memtable-bytes`, `queued-write-bytes`, `decoded-value-bytes`, `scan-page-bytes`,
`max-blocking-operations`, `max-snapshots`, `max-open-databases` and
`disk-reserve-bytes`. The matching environment prefix is
`ARROYO__WORKER__LIVE_STATE_RESOURCES__`, with each kebab-case leaf converted to
underscores. These are one worker-wide pool, not one pool per operator.

### Runnable bounded example

Save this as `native-state.toml`. These values reproduce small native fixture
limits, with room for two live owners; they are an example to qualify, not
production defaults. The worker must be able to create both local directories.
Existing service, connector and authentication configuration still applies.

```toml
checkpoint-url = "file:///tmp/streamr-checkpoints"

[admin]
http-port = 8001

[worker]
sql-state-backend = "rocksdb"

[worker.disk-sql-state]
directory = "/tmp/streamr-live"
max-row-bytes = 24576

[worker.execution-resources]
memory-bytes = 16777216
max-batch-bytes = 1048576

[worker.live-state-resources]
block-cache-bytes = 8388608
memtable-bytes = 4194304
queued-write-bytes = 33554432
decoded-value-bytes = 16777216
scan-page-bytes = 2097152
max-blocking-operations = 2
max-snapshots = 2
max-open-databases = 2
disk-reserve-bytes = 67108864

[worker.typed-sql-state]
key-bytes = 4096
row-bytes = 24576
decoded-bytes = 65536
scope-bytes = 262144
scope-operations = 128
page-bytes = 131072
page-entries = 64
max-working-event-bytes = 262144
max-captured-event-bytes = 131072
max-pending-output-rows = 64
max-pending-output-bytes = 524288
max-resident-bytes = 8388608

[worker.aggregate-state]
key-bytes = 512
value-bytes = 32768
page-bytes = 131072
page-entries = 64
write-bytes = 2097152
write-operations = 128
overlay-bytes = 2097152
max-pending-output-rows = 64
max-pending-output-bytes = 524288
max-resident-bytes = 134217728

[worker.window-state]
key-bytes = 512
partial-bytes = 32768
page-bytes = 131072
page-entries = 64
write-bytes = 524288
write-operations = 64
max-resident-bytes = 134217728
```

With the pinned production executable available as `arroyo` and a caller-owned
supported query in `query.sql`, run a local pipeline at singleton parallelism:

```sh
mkdir -p /tmp/streamr-live /tmp/streamr-checkpoints
arroyo --config native-state.toml run --parallelism 1 --state-dir file:///tmp/streamr-checkpoints query.sql
```

For a configured cluster, start its worker with
`arroyo --config native-state.toml worker`, then submit the same supported
singleton plan through that cluster's normal submission path. Inspect
`curl --fail http://127.0.0.1:8001/metrics` on the worker admin endpoint; local
`run` and a cluster worker have different service lifecycles, so do not infer
worker scrape evidence from query output. To exercise the same bounded native
operators in memory, set `ARROYO__WORKER__SQL_STATE_BACKEND=memory` on a fresh
run and keep the resource/operator blocks. Do not override the backend of an
existing restore without proven compatibility.

Operator limits are independent of backend choice:

- `worker.typed-sql-state`: `key-bytes`, `row-bytes`, `decoded-bytes`,
  `scope-bytes`, `scope-operations`, `page-bytes`, `page-entries`,
  `max-working-event-bytes`, `max-captured-event-bytes`,
  `max-pending-output-rows`, `max-pending-output-bytes`, `max-resident-bytes`.
- `worker.aggregate-state`: `key-bytes`, `value-bytes`, `page-bytes`,
  `page-entries`, `write-bytes`, `write-operations`, `overlay-bytes`,
  `max-pending-output-rows`, `max-pending-output-bytes`, `max-resident-bytes`.
- `worker.window-state`: `key-bytes`, `partial-bytes`, `page-bytes`,
  `page-entries`, `write-bytes`, `write-operations`, `max-resident-bytes`;
  both fixed windows and native SESSION use this block.

`max-resident-bytes` caps the memory adapter's resident state, not RocksDB logical
state. `construct_configured_backend` in `live/worker.rs` selects the common
adapter; SQL execution does not branch into a different SQL dialect. Disk owners
get unique attempt directories under the root and remove local scratch on drop.
Committed checkpoints retain existing schema/encoding/ownership metadata and
recover full logical namespaces. Preserve the remote store and selected
checkpoint; local scratch alone is not a portable recovery artifact.

RocksDB SQL rejects every graph node with parallelism other than one. Native
state-table ownership and SESSION also require singleton ownership in memory.
Larger fixed memory parallelism for another operator is not automatically
qualified merely because its constructor admits a subtask index. Do not rescale
or switch backend/schema/key ownership without explicit compatibility evidence.

## Admission failures and sizing

Construction rejects nonpositive/incompatible limits; execution requires
`0 < max-batch-bytes <= memory-bytes <= isize::MAX`. Aggregate admission needs
room for a maximum key/value, two output rows and at least two writes. Window
admission needs one partial plus indexes and at least four writes. Typed state
admission requires compatible row/decoded, event/capture and output limits.
Runtime stores additionally validate encoded page/cursor, write, decode and
checkpoint reserves. Passing config deserialization alone does not prove a
complete graph can start or process its maximum event.

RocksDB requires disk configuration and explicit live resources. Its row limit
is 1..262144 bytes and must fit derived decoded/write/scan/checkpoint bounds.
Memtable limits must fit the cache limit to which they are charged. Pool budget
changes while active handles exist are rejected to prevent two incompatible
worker pools. Budget exhaustion returns limit errors rather than implicit spill;
execution spill is disabled. Supported graph/network admission shares execution
resources and may reject startup or message delivery when that pool cannot fit
queue metadata, actual payloads and retained work. Arbitrary UDF allocations and
TLS internals are not thereby bounded.

### Invalid configuration examples

Apply each change separately to the example above, on a disposable fresh run.
Some validation happens at graph/backend construction rather than CLI parsing;
submit a query using the affected owner to exercise that check. An invalid
configuration is expected to fail, not silently fall back to memory.

| Change to example | Expected rejection boundary |
| --- | --- |
| Remove `[worker.disk-sql-state]` with backend `rocksdb` | RocksDB SQL requires `worker.disk-sql-state` |
| Remove `[worker.live-state-resources]` with backend `rocksdb` | RocksDB SQL requires explicit worker resources |
| Set `worker.disk-sql-state.max-row-bytes = 262145` | Maximum disk SQL row is 262144 bytes |
| Set `worker.execution-resources.max-batch-bytes = 16777217` | Batch allowance exceeds the 16777216-byte execution pool |
| Set `worker.live-state-resources.max-open-databases = 0` | Live resources must be positive; owner capacity also must fit the actual graph |
| Set `worker.live-state-resources.memtable-bytes = 8388609` | Memtable allowance exceeds the 8388608-byte cache allowance |
| Set `worker.aggregate-state.max-pending-output-rows = 1` | Aggregate admission requires two output rows |
| Set `worker.window-state.write-operations = 3` | Window admission requires at least four writes |
| Set `worker.typed-sql-state.decoded-bytes = 24575` | Typed row allowance exceeds decoded allowance |
| Run RocksDB with `--parallelism 2` | RocksDB SQL requires singleton execution and unchanged parallelism |

All sizes in these config blocks are bytes, not MiB. Output row counts and
operation/owner counts are separate dimensions. Configured maxima must fit
encoded keys, indexes, cursor/metadata overhead and overlapping live buffers;
a value fitting `row-bytes` alone can still fail a stricter operation allowance.

Measure retained logical payload, checkpoint-prefix state and whole-process RSS
separately. Existing fixture budgets are examples, not production defaults:
state-table capacity uses a conservative 48 MiB pool sum; window/SESSION capacity
uses 50 MiB; the examples declare a 512 MiB SQL-test-process RSS envelope.
Membership/history size, number of active owners, output width and checkpoint
transfer concurrency determine required headroom. See the all-key table and
SESSION qualification documents for exact floors, value oracles and pending
current-source runtime status. A successful tiny fixture does not select safe
production defaults, and compressed disk bytes do not measure logical state.

SESSION closes only after watermark exceeds last-event-plus-gap; equality stays
open. Finite EOF can advance event time; an idle or stopped live producer does
not establish an all-idle advancement policy. Native sessions have no explicit
maximum-duration setting. Updating aggregate TTL is retention, and disabling TTL
for lifetime totals does not create autonomous wall-clock emission. Updating
results require a sink that preserves their changelog semantics. Keep caller
collection/value/output limits explicit; rejected collection codecs or silent
truncation are not acceptable operational workarounds.

## Metrics inventory

Production workers register live resources with the default Prometheus registry;
the configured admin HTTP service exposes `/metrics`. Label sets are fixed
resource/measurement, resource/reason, backend/part, operation/outcome,
phase/outcome, direction/outcome or bounded graph identity,
without caller keys or values. Code presence remains distinct from a captured
current-candidate production scrape.

| Signal | Current evidence/source | Limit or remaining gap |
| --- | --- | --- |
| `arroyo_live_state_resources{resource,measurement}` | Admission `used`, `limit`, `waiting`; native cache/memtable refresh; pinned cache, disk available/refusals | Disk available reflects the last admission check, not a continuously refreshed filesystem gauge; use the separate logical and native health families below |
| `arroyo_live_state_admission_refusals_total{resource,reason}` | Existing oversized, closed and exhausted admission errors; reasons are `oversized`, `closed`, `exhausted` | Counts only returned refusals, never cancelled waits; excludes disk/configuration failures and execution-pool admission |
| `arroyo_live_state_admission_duration_seconds{resource}` | Async admission from first poll through acquisition, refusal or cancellation, including immediate outcomes | Includes validation and semaphore wait; excludes time before first poll, synchronous try-admission, permit holding and native work; cancellation contributes a duration sample without a refusal |
| `arroyo_live_state_operation_latency_seconds{operation}` | Fixed read/write/scan/snapshot/open histogram | Native operation time after admission; does not separately report total queue wait or RocksDB write-stall duration |
| `arroyo_live_state_checkpoint_duration_seconds{direction,outcome}` | Logical namespace export/restore durations, success/error/cancelled outcomes | Not entire controller barrier/publication duration or worker startup latency |
| `arroyo_live_state_checkpoint_operations_total{direction,outcome}` | Full logical export/restore operation counts | Requires actual scrape/fault evidence to establish deployment behavior |
| `arroyo_live_state_checkpoint_encoded_page_bytes_total{direction}` | Successfully uploaded/applied immutable checkpoint object bytes, including compressed Parquet; existing metric name retained | Rate gives completed object transfer throughput, excluding manifests, transport overhead and failed-transfer bytes; not live logical state size |
| `arroyo_live_state_logical_keys{backend}` / `arroyo_live_state_logical_bytes{backend,part}` | Current live record count and unencoded key/value payload bytes (`part=key,value`), summed across known memory/RocksDB attempts | Includes internal index/metadata records in registered namespaces. Excludes namespace, routing, encoding and allocation overhead. Check `health_observations{measurement="logical"}` before treating the sum as complete |
| `arroyo_live_state_native_health{measurement}` | Cached `sst_bytes`, `free_bytes`, `write_stopped`, `delayed_write_bytes_per_second`, `sample_age_milliseconds` | Sampled at admitted RocksDB open/write completion. SST is total SST file lengths, excluding WAL/manifests/snapshots; free bytes is the minimum filesystem free space across observed attempts, never summed for shared disks. Stopped databases and delayed-write byte/second rates are summed; sample age is maximum monotonic milliseconds |
| `arroyo_live_state_health_observations{measurement,outcome}` | Live database counts with `available` / `unavailable` measurements | Memory participates only in logical observations. Unsupported/error properties are unavailable, excluded from aggregate values; a zero aggregate with unavailable observations is not a measured zero. Native sample age makes idle/background-compaction staleness explicit |
| `arroyo_worker_execution_memory_bytes{measurement}` | Actual shared pool reserved bytes (`used`), limits (`limit`, `max_batch`) and reservation classes (`spillable`, `nonspillable`), refreshed on scrape | Cooperative reservations in the latest configured pool, not RSS. Class means DataFusion consumer spillability; it does not enable disk spill. Concurrent updates can briefly make class sums differ from `used`. All values read zero after the observed pool dies |
| `arroyo_worker_execution_admission_refusals_total{operation,outcome}` | Cumulative actual pool `try_grow` failures and checked `batch_limit` failures, with `outcome=refused` | No wait queue exists in this execution pool; cancellation is not a refusal. Does not cover untracked allocations or queue-local validation failures that never call the pool |
| `arroyo_worker_lifecycle_duration_seconds{phase,outcome}` / `arroyo_worker_lifecycle_operations_total{phase,outcome}` | Monotonic stage elapsed seconds and success/error/cancelled counts for protocol/metadata publication, worker initialization and startup/restart readiness | Exact boundaries below; phase labels are finite. Neither publication timing includes the preceding distributed barrier nor readiness the preceding controller/process scheduling |
| `arroyo_worker_tx_bytes`, `arroyo_worker_tx_queue_size`, `arroyo_worker_tx_queue_rem` | Payload bytes, configured row capacity, and remaining row capacity respectively | Collector-attached graph queues refresh on admission, drain, failed delivery and receiver teardown (including signals); total capacity is the queue row limit and remains constant when empty. These are payload/row gauges, not shared-pool reservation totals |
| `process_resident_memory_bytes` | Server-common enables Prometheus's process collector feature | Verify actual candidate scrape; harness `wait4`/`/proc` RSS is separate test-process evidence |

Admission labels are limited to `queued_write_bytes`, `decoded_value_bytes`,
`scan_page_bytes`, `blocking_operations`, `snapshots`, `databases` and
`cleanup_slots`. A request waiting for capacity is not a refusal. Dropping its
polled future releases its waiting gauge and semaphore queue position; the
current owner's usage and permits remain held until that owner releases them.
These durations cover individual budget acquisitions, not a complete multi-budget
operation or RocksDB stall. Focused source tests cover these boundaries; they
have not been executed for STR-48 under the requested lint-only validation.

Logical accounting applies only after successful atomic batches: insertion adds
one record and its full logical key/value lengths, replacement changes value
length only, deletion subtracts an existing record, and deletion of a missing
key changes nothing. Repeated operations on one key use the batch's final value.
Failed batches leave counts unchanged. Fresh worker attempts and checkpoint
restore through ordinary puts establish exact baselines. Explicit `reopen` of
an existing local RocksDB directory has an unavailable logical baseline; no
unbounded startup/scrape scan invents it. If a telemetry-only old-value read
fails, logical availability becomes unknown without refusing a valid write.
Closed attempts and snapshots do not contribute live logical counts.

Native sampling calls a fixed number of native properties/statvfs operations
inside existing admitted blocking work; Prometheus collection only reads cached
values and weak observations, with no filesystem scans, spawned collection
tasks or retained database/reservation owners. Unix filesystem availability uses
`f_bavail * f_frsize`; non-Unix availability is unsupported (the existing disk
admission contract also requires Unix). `write_stopped` observes RocksDB's
current stop flag and the delayed rate observes its current slowdown setting;
neither is cumulative stall time or stall-event count. WAL, manifest, snapshot
and total directory allocated disk bytes, cumulative stall duration and storage
device I/O remain explicitly unsupported. Monitor those with host/storage
telemetry and preserve disk reserve; SST lengths are not total local disk use.

Lifecycle `protocol_publication` surrounds exactly `publish_checkpoint`, including
its validation/storage publication work; `metadata_publication` surrounds
`write_checkpoint_metadata` in controller mode. Both start when the future is
first polled and stop when it returns or is dropped, with error and cancellation
kept separate. `worker_initialization` covers `initialize_inner` through engine
construction/phase transition, excluding the subsequent controller notification;
it does not imply operator restore has completed. `startup_readiness` and
`restart_readiness` run from worker `initialize` handling to reception of all
assigned local `TaskStarted` events after operator startup/restore. Restore epoch
or manifest presence selects restart; duplicate/unknown task events cannot
finish early. This includes leader waiting, excludes RPC forwarding after the
last event, and records failure or teardown cancellation exactly once. It is
local worker readiness, not process boot, controller scheduling, global sink
readiness or end-to-end outage duration. Namespace export/restore remains the
separate existing live-state checkpoint metric.

Queue payload/row gauges above and execution reservation classes provide distinct
queue/reservation views. Queue metadata, retained consumer batches, operator and
network transfer reservations are included in their actual DataFusion consumer
classes; per-resource attribution of these reservations is not exposed, and
subtracting payload gauges from total reservations does not produce such a
measurement. Transfer byte rates come from successfully transferred checkpoint
objects and exclude manifest/transport/failed bytes.

Actual candidate `/metrics` scrapes under load, restoration, resource pressure
and cancellation, and measured production sizing/defaults remain the shared
[STR-32](https://trakkt.app/issues/STR-32) qualification. Source fixtures and local
checks establish instrumentation contracts, not production scrape acceptance.

## Source-free SQL capture artifact

`scripts/sql-capture-bundle.py` packages a trusted rebuilt Linux SQL-testing ELF,
its resolved shared libraries/loader, a standalone runner and caller-owned
SQL/input/exact expected rows. It preserves shared-library SONAME filenames and
hashes every packaged file, plus caller-supplied build/source provenance. It
neither compiles nor starts a service. Pick a new artifact directory; existing
nonempty artifacts/results are rejected. The selected ELF must have a dynamic
Linux loader; a static or unsupported-platform binary is rejected.

A fixture manifest supplies `query`, `input`, `expected`, `expected_checkpoint`
paths, `checkpoint_input_rows`, and optional `checkpoint_output_rows`,
`batch_rows`, `timeout_seconds`, `rss_limit_mib`, `ordered`. SQL connector paths
use quoted `{{INPUT}}` and `{{OUTPUT}}` placeholders. Expected files are arrays
of complete JSON objects or streaming `.jsonl`. `ordered=true` preserves exact
source/output ordering; false ignores only complete-row order, preserving array
order and multiplicity. The driver verifies exact JSON types and all values.
Use independently established caller expectations; captured output never creates
an oracle. Idle/scheduled observation manifests are explicitly unsupported by
this minimal runner and must use their dedicated qualification driver.

Prepare/package in the required builder environment using its actual binary,
prepared fixture and retained provenance JSON, then run on compatible Linux with
Python 3.10+ using only the artifact and results mounts. Replace uppercase paths
with explicit paths from the coordinating run:

```sh
python3 scripts/sql-capture-bundle.py package --binary BINARY --fixture FIXTURE_MANIFEST --provenance BUILD_PROVENANCE_JSON --route native-windows --directory NEW_BUNDLE
podman run --rm -v NEW_BUNDLE:/bundle:ro -v NEW_RESULTS:/results:z arroyo-dev python3 /bundle/run.py run --results /results/capture --backend memory --backend rocksdb
```

Select `typed-sql` for state-table/MERGE or `native-aggregates` for native updating
aggregates instead. The runtime command has no `/app` or repository mount. The
artifact's loader uses its packaged library search path. Runtime hashes are
checked before execution, connector paths are resolved against the artifact and
results mount, and each backend/controller/leader capture runs serially with a
finite declared timeout. The child records peak RSS; initial, fresh-worker final
and exact committed-prefix output rows are compared independently. Captures and
hashes go to the new results directory. Re-running this command is a new
qualification capture, not resuming the previous OS process's job.

Each case selects the existing checkpoint URL configuration to retain checkpoint
objects under its results directory. Removing the disposable runtime container
therefore preserves those objects for independent inventory checks. Earlier
captures that used default container-local checkpoint storage retain their
output/log evidence, but do not supply retained checkpoint objects.

The selected test rebuild/source relationship must be supported by independent
build evidence: an ELF hash plus a supplied source revision does not prove it.
Native query planning and checkpoint code are embedded in the test executable,
but it uses test configuration flags and recreates workers inside one OS process.
It is therefore separate from a source-free **production top-level binary**
startup, service/database/broker setup, supported fixed-parallelism plan submission,
committed checkpoint publication, abrupt worker process loss, pinned-candidate
restart and source/sink delivery comparison. Those production gates remain open
in [STR-32](https://trakkt.app/issues/STR-32) on the current native candidate
until an actual run records process/run IDs,
selected restore metadata, source positions, independent outputs and metrics.
The historical external local-service evidence does not qualify today's native
paths or supply required application checkouts.

## Recovery, disk refusal and rollback

For an existing qualified production candidate, retain its immutable executable/
image digest, exact resolved config, frozen SQL/plan, source offsets and sink
contract, selected committed checkpoint metadata/files and controller generation.
After a worker loss, start a fresh attempt with the same supported schema and
ownership and existing checkpoint store. Confirm the selected published epoch
and restored source progress before admitting suffix input; compare actual
outputs with the caller oracle. Never select abandoned uploads or unpublished
attempts by guessing the newest directory. Full protocols govern selection and
exclusive-file retention; preserve their referenced files and cleanup evidence.

On disk refusal, record the exact resource/error, root filesystem free bytes,
configured reserve, active owners and checkpoint operation. Check whether local
scratch, checkpoint files, logs or other writers consumed headroom; the reserve
check cannot reserve against concurrent external writers or compaction growth.
Increase capacity or reduce declared admissible work after reviewing evidence;
do not delete active scratch or committed checkpoint files to make a test pass.
Checkpoint integrity/publication and cleanup failures require their own fault
qualification; this bundle does not inject them.

Rollback means restarting the pinned prior candidate with its proven compatible
config/plan/checkpoint, preserving independent outputs and source/sink guarantees.
If the pinned candidates have different checkpoint schema/ownership, do not
restore the new checkpoint into the old executable. Use the prior candidate's
qualified checkpoint and source/sink replay contract, or stop for a caller-owned
cutover decision. Swapping executables or renaming operators is not compatibility evidence. This runbook makes no
production cutover or data-retention change.
