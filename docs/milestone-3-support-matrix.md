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

## Revised delivery scope

The current milestone plan uses native SQL operators before proposing new
extensions. This is planned scope, not a claim that the paths below are already
implemented or qualified:

| Capability | Milestone 3 owner | Required evidence |
| --- | --- | --- |
| Typed state tables, current-row reads and continuous MERGE | [STR-38–42](https://trakkt.app/issues/STR-38) | Separate retained storage/output relations, one ordered state owner, source/old/new/no-action output and native recovery |
| Persistent updating aggregates | [STR-17](https://trakkt.app/issues/STR-17) | Exact COUNT/SUM/extrema/ordered first-last/FILTER/NULL values, bounded retraction state and explicit retention |
| Native TUMBLE and HOP panes and closure | [STR-19](https://trakkt.app/issues/STR-19) | Configured-backend migration, watermark/expiry/quiet-key behavior and bounded open-window recovery |
| Native SESSION | [STR-20](https://trakkt.app/issues/STR-20) | Gap/late-input/deadline/max-duration semantics, bounded histories and closure recovery |
| Typed aggregate/window composition, ranking and arrays | [STR-29](https://trakkt.app/issues/STR-29) | Runnable native plans, bounded values/collections and precise composition/emit behavior |
| Legacy state_* SQL function removal | [STR-43](https://trakkt.app/issues/STR-43) | Native caller migration, explicit old-plan/checkpoint handling and removal from active catalogs/examples |

[STR-28](https://trakkt.app/issues/STR-28) freezes the actual native plans and
maps each required behavior to supported SQL, a demonstrated defect or a policy
difference requiring discussion. A non-windowed GROUP BY already retains
aggregate state; a whole-record JSON processor is not the default replacement.
Native TUMBLE/HOP work is now in milestone 3. Broader analytic histories,
unsupported aggregate variants, general historical joins and rescaling remain
milestone 4 unless a selected plan demonstrates a required narrow gap.

All selected paths must use backend-neutral state interfaces and the configured
live SQL backend: memory and RocksDB currently. New providers belong behind the
construction/lifecycle/capability boundary, not in SQL operator logic. Existing
Native aggregate, window, session and state-table owners now use one configured
backend construction adapter in `live/worker.rs`. Legacy state-function execution
remains a separate path until STR-43 removal; future providers belong in the
construction adapter rather than these SQL owners.

The five legacy functions are state_get, state_put, state_upsert, state_update
and state_delete. They remain in the current code until STR-43's prerequisites
pass; ordinary native table INSERT/MERGE and backend get/put APIs are retained.
Historical milestone 2 evidence is preserved as evidence of the old path.

No new SQL timer/callback API is preselected. Native windows already schedule
their own closure. Application emission/deadline requirements must be tested
against native composition before proposing an additional generic interface.
Application SQL, schemas and oracles stay in the consuming repository. Proposed
external SQL is not physical-plan/native-runtime evidence.

## Native route evidence

The [STR-28 native capability audit](milestone-3-native-capabilities.md) maps all
external profile/session fields and lifecycle rules, records source-level
composition restrictions, retained ownership and the STR-43 removal inventory.
Its source review is based on an earlier implementation snapshot; current tested
routes and limitations are in the [validation record](milestone-3-validation.md).
Generic ordered FIRST_VALUE/LAST_VALUE value and recovery cases now pass on the
configured memory/RocksDB paths. That does not establish application arrival
ordering or full profile/session parity. Memory and RocksDB evidence remains
path-specific, and native SESSION semantics are not assumed equivalent to an
external timer-driven function.

On PR #6 head `d19c30e3`, generic UUID, closed-HOP reaggregation and
lifetime/latest-result composition each pass eight backend/batch/protocol
captures. External identity passes eight captures and 16 strict
initial/recovered comparisons. A separate application-owned 14-field
profile-core probe passes eight captures, but the full 33-field profile,
12-session behavior and emission timing remain unqualified. Existing-SQL
`LAG` session and updating-left daily join probes still fail planning; one
aliased `DATE` grouping probe passes memory/batch-1/controller only. See
[the current validation record](milestone-3-validation.md) for artifact paths
and exact limits.

On the later test harness, published in `f58be084`, eight application-owned three-event
processing-time TTL cases passed strict initial/recovered idle snapshots: an
indefinite lifetime branch stayed at three while a selected four-second
latest-result branch expired from two to zero; the all-finite negative control
deleted its row. This does not qualify event-time profile decay or timers.
An application-owned keyed session `MERGE RETURNING` flags query passed eight
backend/batch/protocol captures; its native `SUM` totals variant initially
encountered a planner ArrowKey fusion rejection. After a generic fusion repair,
the full 16-case flags/totals matrix passed strict capture and recovery
comparisons on a fresh source with 619 library tests (four ignored). The
selected totals reach two after the checkpoint prefix and three at the final
source row; inactivity and complete-session rules remain unqualified. Four
small HOP memory/RocksDB × controller/leader full-payload recovery smokes
also passed. A subsequent 65,000-key RocksDB controller HOP run matched all
130,000 item/window pairs and full payloads after a 64,999-row checkpoint
and fresh-worker recovery: retained payload exceeded 10× the declared
50 MiB pool sum, with 355,061,760-byte peak RSS below the 512 MiB cap.
Strong HOP leader qualification remains pending. Lifetime
`COUNT → ROW_NUMBER` page/feature probes and an already-ranked ordered
`ARRAY_AGG` probe originally failed at planning/construction. The later
bounded ARRAY_AGG repair now passes eight typed CDC recovery cases and eight
external already-ranked array cases; lifetime ROW_NUMBER remains unsupported.
There is no ranked value/recovery qualification. See
[validation](milestone-3-validation.md) for exact artifacts and gates.

The approved timestamp repair and UNION consolidation passed 24 generic
count/MAX update/delete/recovery cases across memory/RocksDB, controller/leader
and source batches 1/8. The same source passed eight typed-array, eight
empty-array and 16 selected external compatibility captures, with 636 library
tests and all five development gates. Grouped rows disappear after their final
contribution is removed. The engine-injected timestamp MAX retracts its original
member by updating row ID; caller aggregates retain exact value semantics.
Old incompatible native state codecs fail closed. See the
[timestamp regression](updating-aggregate-timestamp-regression.md) and
[typed array evidence](native-updating-array-cdc.md). This is a qualified slice;
full profile/session, lifetime ranking and broader milestone gates remain open.

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
| `arrow/session_aggregating_window.rs`, `session_native.rs`, `session_store.rs` | Legacy per-key maps; selected native path uses paged raw rows, session/deadline metadata and bounded final-input delivery | Native memory/RocksDB value and fresh-worker recovery captures pass for scalar aggregates and exact-gap/bridge handling; direct late-drop probes also pass at batch target 1. A 65,000-row hot SESSION passes both checkpoint protocols with a retained-payload floor above 10× the declared 50 MiB pool sum, including a 64,999-row checkpoint. High-cardinality sessions, broader late/idleness behavior, collections, backpressure and fault qualification remain open. |
| `arrow/tumbling_aggregating_window.rs`, `sliding_aggregating_window.rs`, `window_native.rs`, `window_store.rs` | Legacy per-bin maps; selected native path uses paged panes/partials and closure/expiry indexes | Scalar and ordered memory/RocksDB SQL and recovery captures pass. An earlier snapshot-reuse candidate passed 65,000-key TUMBLE initial and recovered full-payload/RSS checks in both protocols, but its halfway checkpoint was below 10×. On `d19c30e3`, the stronger 64,999-row RocksDB checkpoint passed controller restore and all 65,000 full-payload comparisons above the 10× floor at 355,414,016-byte peak RSS; leader failed the unchanged 3 MiB checkpoint-metadata cap. The subsequent compact-basename and bounded idle-harness build passed workspace checks and 614 library tests (four ignored), then repeated the 64,999-row checkpoint under the leader protocol: all 65,000 full payloads matched after fresh-worker recovery at 361,074,688-byte peak RSS. The later test-only idle pre-match build passed 616 library tests (four ignored), but the leader capacity run used the earlier binary. These controller and leader results are from their respective tested source revisions; the 3 MiB metadata cap remains unchanged. A stronger HOP controller run on the later fused source passed the 10× checkpoint floor and full-payload recovery at 355,061,760-byte peak RSS; strong HOP leader and broader quiet-key/idleness qualification remain open. Selected collection cases for ARRAY_AGG, DISTINCT aggregates and single-column UNNEST pass, including oversized-value rejection; broader collection shapes, capacity and backpressure remain open. |
| `arrow/incremental_aggregator.rs`, `aggregate_store.rs` | Selected native path persists accumulators, counted extrema, ordered members, dirty output and expiry state | Both configured backends pass small updating-input value/recovery captures; indefinite retention is explicit. Hot-key/high-cardinality, backpressure and fault qualification remains open. |
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

- **External callbacks (broader design topic):** what supported extension interface lets external code
  execute bounded stateful callbacks, with cancellation and checkpoint ordering?
- **Timer registration and dispatch:** how are generic timer IDs/payloads exposed
  to external consumers, and who owns event/processing-clock advancement,
  revalidation, overdue restore behavior and watermark forwarding?
- **Schema ownership:** how do external consumers supply/version input, output
  and durable-state schemas without application models entering engine crates?
- **Operator registration:** how can independently owned logic be registered and
  planned through a generic interface, without adding application-specific
  protocol variants or built-in worker operators?
- **Coordinated state and checkpoint assembly:** how are namespaces registered for
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
