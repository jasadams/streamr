# Milestone 3 generic retained-state support

This document inventories Streamr capabilities and remaining integration gaps.
It does not define application schemas, business rules, entity ownership or an
application-specific operator design. Applications depend on Streamr's generic
interfaces; Streamr must not depend on an application repository or its models.
Interface gaps require discussion before implementation.

The starting implementation is milestone 2 `fbd5179a`. Milestone 3 adds generic
execution accounting, durable timer indexes and ranked collections. Storage
capability tests and an external workload capture do not establish migration of
all retained-state operators, delivery guarantees or unrestricted beyond-RAM
execution. See [validation evidence](milestone-3-validation.md).

## Retained-state inventory

| Path / node | Retained working state | Current support and limitation |
| --- | --- | --- |
| `crates/arroyo-worker/src/arrow/stateful_processor.rs` / `StatefulProcessor` | Singleton ordered named scalar maps; one RocksDB execution owner | Disk working state and full logical checkpoints are supported. Reads return complete bounded values. The disk SQL path supports primitive and UTF-8 rows and a bounded function set; it does not expose native collection outputs or durable callback scheduling. |
| `crates/arroyo-state/src/live/{mod,rocks,memory,table}.rs` | Namespaced key/value state, atomic multi-key batches and stable snapshots | Reusable backend interfaces with owned bounded reads and cursor scans. `LiveTable::write_batch` rejects cross-table operations; cross-namespace atomic batches require the shared backend and caller serialization/admission. |
| `crates/arroyo-state/src/live/timers.rs` / `DurableTimers` | Primary timer records and deadline indexes inside one namespace | Atomic replacement/cancellation, separate clock kinds, bounded due pages and stale-entry revalidation. This is storage support; no worker callback scheduler or SQL timer functions are supplied. |
| `crates/arroyo-state/src/live/collections.rs` / `RankedCounts` | Separate member counters and ranking records inside one namespace | Checked counters, atomic prepared updates, bounded member/rank scans, exact bounded top-K and incremental cleanup. Collection values are not growing serialized blobs. Existing SQL map functions do not automatically expose these APIs. |
| `crates/arroyo-state/src/live/time.rs` / `ArrowHistory` | Chunked Arrow history and timestamp-first expiry records in derived namespaces | Bounded chunks, history pages and expiry batches. This is not a timer scheduler or a migrated window operator. Its derived namespaces need explicit checkpoint integration; creating a history from a registered table does not automatically register/export those namespaces. |
| `crates/arroyo-state/src/tables/table_manager.rs`, `live/checkpoint.rs` | Registered disk namespaces, shared barrier snapshots and exclusive logical page files | Restores selected remote state into a fresh attempt and exports full registered namespaces with checksums/schema/ownership. Sharing a database does not make unregistered namespaces durable. |
| `crates/arroyo-worker/src/arrow/execution.rs`, `arrow/sync/streams.rs`, `StatelessPhysicalExecutor` | Shared DataFusion runtime, input/output reservations and bounded batch delivery | Opt-in cooperative execution accounting and per-batch checks. Spilling is disabled. Arbitrary UDF allocations, queues and legacy retained structures are not universally covered. |
| `crates/arroyo-operator/src/operator.rs` / `ArrowOperator` | Serialized batch, watermark, tick and checkpoint callbacks | Generic execution hooks exist. Their presence does not provide durable timer registration, dispatch or application callback transactions. |
| `arrow/session_aggregating_window.rs` / `SessionWindowAggregate` | Per-key computations, start/deadline maps, raw batches and unbounded internal channels; legacy tables | Generic SQL session windows remain an existing separate capability. They require retained-state migration and bounded scheduling before admission under the disk SQL setting. |
| `arrow/tumbling_aggregating_window.rs`, `sliding_aggregating_window.rs` | Per-bin execution maps, panes/batches and internal channels | Selected retained/partial-aggregation paths still require migration and qualification. |
| `arrow/incremental_aggregator.rs` | Accumulator/key cache, changed-key sets and updated values | Persisted bounded accumulators and incremental changed-key iteration remain needed. Checkpoint files alone do not bound these structures. |
| `arrow/instant_join.rs`, `join_with_expiration.rs` | Execution holders/streams and legacy retained-table access | Unsupported under the disk SQL setting until the exact working-state paths are migrated and tested. |
| `crates/arroyo-worker/src/engine.rs` | Operator construction and worker admission | Rejects unsupported retained-state operators under RocksDB SQL and checks database-owner capacity. Keep these gates until each generic path has its own evidence. |

## Generic API contracts and limits

### SQL maps and checkpoints

Disk SQL maps currently require singleton parallelism, subtask 0 and one fresh
backend per execution owner. All registered live tables in an operator share that
backend. There are at most 32 maps and 65,536 exclusive page files per operator
checkpoint, also subject to a 3 MiB serialized whole-subtask metadata limit below
the existing 4 MiB RPC receive limit. The extra 3 MiB cap applies to disk-enabled
subtasks, including their legacy metadata; memory-only metadata retains existing
behavior. Encoded disk namespaces are limited to 512 bytes and schema identities
to 1,024 bytes. Table names must satisfy registration/path validation.

Every namespace containing durable records must be represented in restore and
checkpoint metadata. Namespace bytes must match the configured table name.
Timer and collection indexes deliberately stay inside their view's namespace.
History uses derived namespaces, so its factory alone is insufficient to connect
it to the current registered-table export path.

Configured row/value, encoded batch, read, scan, snapshot, database and disk
budgets apply to their respective operations. They do not cap total process RSS.
SQL values are owned whole-value reads; moving a growing value to disk does not
make decoding or rewriting it incremental. Full snapshot export is bounded by
pages, but checkpoint duration and metadata still grow with state size.
Controller/leader checkpoint selection and fencing are authoritative; local WAL
is not a source for selecting committed state.

### Durable timers

`DurableTimers` accepts bounded IDs and payloads and caller-defined signed
deadline units. `TimerClock::Event` and `TimerClock::Processing` are independent
indexes. `replace` and `cancel` update primary/index records in atomic batches.
Prepared operations allow a serial caller to combine timer changes with other
state mutations in one backend batch. The caller must exclude competing writers
from preparation through commit; there is no backend compare-and-swap here.

`TimerSnapshot::due(clock, through, cursor)` returns deadlines at or before the
cutoff in bounded pages, including many IDs at the same timestamp. Cursors belong
to one snapshot, clock and cutoff. Before acting on a snapshot entry, the caller
must revalidate it against live state and serialize the subsequent mutation.
`TimerPage::entries()` borrows admitted buffers; copies retained beyond the page
need separate admission.

`with_resources` validates room for one retained page plus revalidation reads
and for native scans, including escaped keys and cursor overhead. Multiple held
pages, concurrent producers and composed transactions require additional caller
admission. The view does not choose watermark progression, clock recovery,
callback ordering, overdue dispatch or output delivery policy.

### Ranked collections

`RankedCounts` stores unsigned 64-bit nonzero counters separately from ranking
records; zero is represented by absence. Addition checks overflow/underflow.
Ranking is count descending, then member bytes ascending. This ordering is a
generic API contract, not a promise of compatibility with another runtime's
iteration order.

Entity/member, page, top-K and encoded-batch limits are explicit.
`PreparedCountUpdate::append_to` validates a combined batch before transferring
operations; keep its reservation alive through commit. One serial view must own
updates between the initial read and commit. Pages and top-K results expose
borrowed entries while retaining their reservations. `with_resources` accounts
for simultaneous prepared/result buffers and native read/scan headroom.

`clear_page` removes one bounded page and both indexes atomically. It is
incremental deletion, not an atomic logical reset. Callers must prevent writes
to the entity being cleared and persist any logical retirement/cleanup progress
they require. A checkpoint can capture an intermediate cleanup state; whether
that state is logically retired is a caller-owned contract.

### Histories and execution

`ArrowHistory` applies chunk-byte/row, page-byte/entry and batch limits. Primary
history and expiry records update together, and snapshot reads are stable.
Appending multiple chunks can leave a committed prefix after cancellation;
caller-supplied key/time/sequence identities govern retries. Its resource
reservations and derived-namespace export requirements must be reviewed for the
particular integration rather than inferred from timer/collection tests.

`worker.execution-resources` provides a shared cooperative DataFusion memory
pool and `max-batch-bytes`. Reservations accompany the selected executor's input
lifecycle and yielded output. Output checking happens after the producer has
allocated a batch; cooperative operators must use the same pool. Holding/cloning
outputs beyond their consumption boundary needs caller accounting. This does not
bound arbitrary UDF allocations, channel contents or legacy window/join state.

## Unresolved generic interface gaps — discussion items

The following are questions to resolve with interface consumers before choosing
or implementing a design. They are not approved application integrations:

- **External callbacks:** what supported extension interface lets external code
  execute bounded stateful callbacks, with cancellation and checkpoint ordering?
- **Timer registration and dispatch:** how are generic timer IDs/payloads exposed
  to external consumers, and who owns event/processing-clock advancement,
  revalidation, overdue restore behavior and watermark forwarding?
- **Schema ownership:** how do external consumers supply/version input, output
  and durable-state schemas without application models entering engine crates?
- **Operator registration:** how can independently owned logic be registered and
  planned through a generic interface, without adding application-specific
  protocol variants or built-in worker operators?
- **Bounded multi-namespace transactions:** how are namespaces registered for
  export/restore, producer reservations retained, multiple prepared operations
  admitted and related state/output/checkpoint boundaries expressed? This also
  needs a supported integration for derived history namespaces.

Any proposed extension must identify its retained structures, resource limits,
error behavior and recovery semantics. Generic storage atomicity does not by
itself establish callback/output atomicity or connector exactly-once delivery.
Existing source/sink contracts require independent qualification.

## External evaluation provenance

An external identity workload capture from
[PR 5](https://github.com/jasadams/streamr/pull/5) was integrated at `7637602e`.
Separately maintained reference fixtures informed the capability inventory.
These are external evaluations of generic interfaces; their schemas, business
rules, ownership choices and compatibility decisions belong to the application
repository. They do not authorize built-in application operators or define
Streamr platform policy. Platform verification must remain runnable without an
application checkout; application parity is a separate opt-in acceptance suite.
