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

State-table ownership and native SESSION currently reject parallelism other
than one. Larger fixed parallelism for another operator is not automatically
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
resource/measurement, resource/reason, operation, direction/outcome or bounded graph identity,
without caller keys or values. Code presence remains distinct from a captured
current-candidate production scrape.

| Signal | Current evidence/source | Limit or remaining gap |
| --- | --- | --- |
| `arroyo_live_state_resources{resource,measurement}` | Admission `used`, `limit`, `waiting`; native cache/memtable refresh; pinned cache, disk available/refusals | Disk available reflects the last admission check, not a continuously refreshed filesystem gauge; logical table cardinality/bytes is not supplied |
| `arroyo_live_state_admission_refusals_total{resource,reason}` | Existing oversized, closed and exhausted admission errors; reasons are `oversized`, `closed`, `exhausted` | Counts only returned refusals, never cancelled waits; excludes disk/configuration failures and execution-pool admission |
| `arroyo_live_state_admission_duration_seconds{resource}` | Async admission from first poll through acquisition, refusal or cancellation, including immediate outcomes | Includes validation and semaphore wait; excludes time before first poll, synchronous try-admission, permit holding and native work; cancellation contributes a duration sample without a refusal |
| `arroyo_live_state_operation_latency_seconds{operation}` | Fixed read/write/scan/snapshot/open histogram | Native operation time after admission; does not separately report total queue wait or RocksDB write-stall duration |
| `arroyo_live_state_checkpoint_duration_seconds{direction,outcome}` | Logical namespace export/restore durations, success/error/cancelled outcomes | Not entire controller barrier/publication duration or worker startup latency |
| `arroyo_live_state_checkpoint_operations_total{direction,outcome}` | Full logical export/restore operation counts | Requires actual scrape/fault evidence to establish deployment behavior |
| `arroyo_live_state_checkpoint_encoded_page_bytes_total{direction}` | Successfully uploaded/applied immutable checkpoint object bytes, including compressed Parquet; existing metric name retained | Rate gives completed object transfer throughput, excluding manifests, transport overhead and failed-transfer bytes; not live logical state size |
| `arroyo_worker_execution_memory_bytes{measurement}` | Actual shared pool reserved bytes (`used`), configured memory limit (`limit`) and batch limit (`max_batch`), refreshed on scrape | Cooperative reservations in the latest observed configured pool, not RSS or graph/network breakdown; reservations surviving in a replaced pool are not summed; all three read zero when the observed pool is gone; no admission wait/failure counter |
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

Remaining STR-26 instrumentation includes exact live logical state size,
continuous free/local disk usage, explicit I/O stalls, execution admission failures
and graph/network reservation breakdown, total checkpoint bandwidth/
publication duration and full worker restart time. Existing counters do not fill
those gaps by renaming them. Production defaults remain a measured deployment
sizing deliverable. This doc does not claim the full STR-26 ticket is complete.

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
on the current native candidate until an actual run records process/run IDs,
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
If the selected plans have different checkpoint schema/ownership,
rollback needs an application-approved replay path; swapping executables
or renaming operators is not compatibility evidence. This runbook makes no
production cutover or data-retention change.
