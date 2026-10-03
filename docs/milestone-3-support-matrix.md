# Milestone 3 retained-state support and application contracts

This is an implementation inventory and proposed integration boundary, not a
claim that sessions or profiles execute on disk. The starting Streamr candidate
is milestone 2 `fbd5179a`; the identity capture from [PR 5](https://github.com/jasadams/streamr/pull/5)
has been integrated onto `feat/str-milestone-3` at `7637602e`. The identity capture
does not exercise session or profile timers.

The production reference inspected is Arcstream `365e6eb00162e671366b23175929b00e7e59c135`.
Independent reference worktrees are ARC-15 profile
`583ec4126597fe6b33ac2d4b254169e833dc49e3` and ARC-16 session
`10f779469748d0c0dde6d5af3aa7375f8dbc36d3`. Their strict comparators and resolved
fixtures live in `test/streamr-reference/`. Pin a new revision and source hashes
when application-reference agents finish their current work.

## Actual Streamr paths

| Path/node | Retained working state today | Milestone 3 boundary |
| --- | --- | --- |
| `crates/arroyo-worker/src/arrow/stateful_processor.rs` / `StatefulProcessor` | Singleton, ordered named scalar maps; RocksDB through one execution owner; complete logical disk snapshots | Existing reusable owner/export foundation. Does not provide application timers, lists/maps, ranking or native profile/session schemas. |
| `crates/arroyo-state/src/live/{mod,rocks,memory,table}.rs` | Atomic bounded multi-key backend batches, stable snapshots, namespace-scoped reads and bounded cursor scans | Shared primitives for metadata, collections, timer pointers and indexes. Atomic batches spanning namespaces must use the shared backend; `LiveTable::write_batch` intentionally rejects cross-table operations. |
| `crates/arroyo-state/src/live/time.rs` / `ArrowHistory` | Chunked Arrow primary history plus atomic timestamp-first expiry index | Generic retained history primitive, with bounded scans and expiry pages. It is not a timer scheduler and does not make existing window operators disk-backed. |
| `crates/arroyo-state/src/tables/table_manager.rs`, `live/checkpoint.rs` | Registered disk namespaces share one attempt backend and one barrier snapshot; full pages carry checksums/schema/owner/epoch | Register every namespace used by a new operator before restore. Export all indexes and scalar state together. Preserve current metadata/file/transport limits. |
| `crates/arroyo-operator/src/operator.rs` / `ArrowOperator` | Serialized batch, watermark, tick and checkpoint callbacks; chained operators expose minimum tick interval | Runtime entry points for dual-clock durable timers. No synthetic input event is needed. Timers must finish due work before forwarding the triggering watermark. |
| `arrow/session_aggregating_window.rs` / `SessionWindowAggregate` | `key_computations`, deadline/start maps, raw Arrow batches and unbounded internal channels; legacy expiring/global tables | Requires real retained-state migration and bounded scheduling. Existing SQL session semantics differ from Arcstream's function; migrating storage alone does not establish application compatibility. |
| `arrow/tumbling_aggregating_window.rs`, `sliding_aggregating_window.rs` | Per-bin execution maps, buffered panes/batches and unbounded channels | Inventory and migrate the selected partial-aggregation/retained-batch path; qualify cold and hot keys separately. |
| `arrow/incremental_aggregator.rs` | Accumulator/key cache, changed-key sets and updated values | Needs bounded persisted accumulators and incremental changed-key iteration. Writing checkpoint files does not bound these structures. |
| `arrow/instant_join.rs`, `join_with_expiration.rs` | Execution holders/streams and legacy retained table access | Must remain explicitly unsupported under the disk setting until their selected paths are migrated and tested. |
| `crates/arroyo-worker/src/engine.rs` | Startup rejects unsupported retained-state operators under RocksDB SQL | Keep this gate until each exact operator path is admitted. Enabling a new application operator must not accidentally enable generic RAM windows/joins. |

Milestone 2 disk-map metadata supports singleton subtask 0, complete exclusive
file lists, at most 65,536 files and a 3 MiB whole-subtask metadata envelope.
New timer/collection namespaces consume the same envelope. The registered table
name must match its encoded namespace exactly: hidden timer index namespaces
cannot be assumed to export merely because they share a RocksDB database.

## Minimal compatible application integration

First implement reusable bounded timer, collection and ranking primitives. Then
add dedicated, versioned typed session/profile operators selected by an explicit
planner node/API configuration. They can reuse the live backend and checkpoint
protocol without serializing a whole growing session/profile into one SQL map
value. Generic SQL `SESSION` and opaque `profile_step` JSON UDFs are not equivalent
substitutes for these contracts.

Each application operator should own one singleton backend. Register a small,
fixed namespace set for scalar metadata, timer pointers, event-time deadline
index, processing-time deadline index, collection entries and collection cleanup
progress. Profile ranking and daily buckets can use additional fixed namespaces.
Each event updates affected metadata, count/membership records, old timer deletion
and new timer insertion in one bounded backend batch before completing the row.
Every string/key, encoded scalar, batch, scan page, output and counter must have
an explicit admission/overflow bound.

A deadline index key should sort by clock kind, signed millisecond deadline,
encoded tenant/entity key and timer kind/incarnation. The scalar timer pointer
records the current deadline/incarnation. Replacement deletes the old index key
and inserts the new pointer/index atomically. Due scans read one bounded page,
check the pointer before firing, and atomically consume/update the timer and
application state. Never retain a heap/map containing all deadlines in RAM.
Distinct timer kinds at one timestamp must survive independently.

`handle_watermark` drains event-time deadlines `<= watermark`; `handle_tick`
drains processing-time deadlines `<= current wall time`. Bound each scan/output
batch and cooperate with backpressure. If draining spans callbacks, persist or
otherwise correctly serialize its frontier; do not forward an event watermark
past unprocessed due timers. After fresh restore, processing-time deadlines that
are already overdue must fire without another input. Event-time deadlines must
use the restored/received event watermark, never wall-clock time. All-producer
idleness does not advance event time; finite EOF can provide the final watermark.

Capture metadata, pointers, indexes, collections, last-emitted profile snapshot
and any cleanup/firing progress in the same barrier snapshot. Existing controller
and leader selection/fencing remain authoritative; local WAL is disposable.
Test cancellation and failure before/after state mutation, timer removal, output
and checkpoint publication. Callback atomicity plus source/sink checkpoint
alignment must be demonstrated; it does not establish Kafka exactly-once delivery.

## STR-20 session contract

Production sources are `flink/identity-resolution/src/main/java/com/pipeline/session/`
`SessionFunction.java`, `SessionState.java`, `SessionSummary.java` and
`SessionizationJob.java` in Arcstream.

- The first arriving record owns `session_id`, `canonical_id`, `tenant_id` and
  start time. End time is the maximum accepted event time, without adding a gap.
- Every arriving event increments the count, including older arrivals. The
  last nonempty arriving device/browser/country value wins. Pages are distinct;
  event-type counts include every accepted record.
- Activity replaces the single event-time deadline with `last_event + 30 min`.
  Closure occurs at watermark **equal to or greater than** that deadline.
  A long event-time gap before watermark closure does not itself split a session.
- Closure emits once and clears the active session. Reusing an ID later starts
  a fresh session; pages/counts must not leak across incarnations.
- The source derives timestamps from payload `event_time`, allows five seconds
  of bounded disorder and marks partitions idle after thirty seconds. Its
  current key is session ID alone.

The exact 12-field output is eight strings (`session_id`, `canonical_id`,
`tenant_id`, `start_time`, `end_time`, `device_type`, `browser`, `country`), integer
`duration_sec` and `event_count`, native `pages: List<Utf8>` and native
`event_types: Map<Utf8, integer>`. Times are UTC strings
`yyyy-MM-dd HH:mm:ss.SSS`; duration is `(end - start) / 1000` using integer division.
The session comparator normalizes page order, but requires distinct pages and
exact integer counts. JSON strings containing arrays/maps fail the contract.

Persist one bounded scalar session header. Store each page membership and each
event-type counter under separately addressable keys. On close, build the native
output using admitted, bounded scans and a checked output-size reservation.
Keep a durable incarnation when logically clearing an entity; retire collection
prefixes using bounded cleanup pages/cursors. An unbounded delete batch or clearing
the incarnation before old collection cleanup would mix a reused ID with old data.

The complete pages/type-count output can grow without bound. Consumer schemas do
not supply a safe truncation rule: the strict reference requires the full fields,
even though current Pinot session schema omits pages/type counts. The candidate
must either fail explicitly at a declared output/collection limit or adopt an
approved different output contract. Never silently drop pages/types or replace
the native fields with JSON strings. Checked count overflow should likewise fail
explicitly rather than producing wrapped/NULL counters.

## STR-29 profile contract

Production sources are Arcstream `.../profile/ProfileFunction.java`,
`UserProfileState.java`, `ProfileUpdate.java` and `ProfileUpdaterJob.java`.

Creation emits immediately. Subsequent activity starts one processing-time
debounce at current processing time plus five seconds when no debounce is pending;
later events do not extend that pending deadline. Activity replaces its
event-time session timer and cancels pending 1/7/30-day decay timers. Importantly,
the session deadline uses the **current event's** clamped timestamp plus thirty
minutes, while `last_seen` remains the maximum timestamp. An older accepted event
can therefore move the session deadline earlier; replacing this with `last_seen`
would change the reference.

On session timeout, close the active session, add nonnegative
`last_seen - session_start` to completed duration, and schedule event-time decay
at `last_seen + 1/7/30 days`. Processing-time debounce uses the date of `last_seen`
for bucket sums; event-time timeout/decay uses the callback timestamp's UTC date.
Processing-time and event-time timers at the same timestamp require independent
dispatch. A profile without an active session does not fabricate a timeout output.

The exact 33-field output has 19 integer fields (timestamps/counters/window sums/
durations), ten strings, one boolean and three native string arrays
`top_pages`, `top_features`, `changed_fields`. The strict profile comparator
normalizes only changed-field order; top-K order and every type remain exact.
Current consumer Pinot profiles use `(tenant_id, canonical_id)` as their primary
key, native multi-value page/feature arrays and LONG counters. Query API models
also expect real arrays and numeric counters. Do not serialize these as JSON strings.

`changed_fields` compares only the persisted last **emitted** snapshot's selected
fields, not the last input. Creation uses the fixed five-field set. Window decay
can emit an empty changed-field list even though window sums changed; expanding
that list would change the baseline. `signups` is retained internally but is not
one of the 33 output fields.

Separate page/feature counts from scalar metadata. Maintain an atomic ranking
index alongside each count update, so exact top-5 pages/top-3 features read bounded
prefixes rather than loading the full hot profile. Daily event/session-start
buckets are separate bounded records; preserve UTC-date sums and pruning rules.
Completed-session totals, active session metadata, all five timer pointers and
the last-emitted snapshot are durable scalar state.

## Decisions that must be explicit

| Decision | Current baseline / consequence |
| --- | --- |
| Tenant ownership | Flink keys session by `session_id`, profile by `canonical_id`; collisions can cross tenant boundaries. Tenant-qualified ownership is a baseline correction, also requiring downstream session primary-key coordination. The coordinating agent selected tenant-qualified ownership as the default after explaining customer separation; downstream session primary-key coordination remains required before readiness. |
| Historical/malformed time | Session accepts valid historical payload time and falls back to wall clock for malformed timestamps. Profile clamps timestamps older than 91 days or over 60 seconds in the future to wall clock; daily pruning also uses wall-clock UTC date. Preserve or explicitly approve changing these replay-sensitive behaviors. |
| Profile top-K ties | Java sorts only by descending count and inherits `HashMap` encounter order for equal counts. That is not a portable deterministic tie contract. Recommend count descending, UTF-8 key ascending as an explicit correction, with competing/tied-rank fixtures. A one-page reference cannot decide this. |
| Session complete collections | Define admitted element/output limits and explicit failure, or approve another output shape. The existing full-summary contract cannot be bounded through truncation. |
| Clock-derived fields | Profile `updated_at`, `timestamp` and active-session duration use wall clock. Preserve measured emission-clock bounds; do not claim bitwise replay determinism for those fields. |
| Delivery | Existing Kafka offset checkpoints and incomplete commit-phase recovery are separate gates. File-sink recovery and passing Flink oracles do not establish production connector delivery guarantees. |

## Slice order and acceptance evidence

1. Inventory and qualify generic persisted accumulators/history/timer primitives;
   keep unsupported operator gates. Prove bounded hot/cold state and checkpoint
   restore in controller and leader modes before selecting application nodes.
2. Implement the typed session slice against ARC-16's three-emission, 12-field
   resolved fixture: restore before replacement deadlines, before/after closure
   and after cleared-state ID reuse; assert silence before the exact boundary.
3. Implement the typed profile slice against ARC-15's six-emission, 33-field
   fixture: creation, debounce, timeout and 1/7/30-day decay, restoring before
   pending timers and emitting without input. Capture actual wall-clock bounds.
4. Extend to tenant collisions, long gaps, late records, malformed/future/historical
   timestamps, equal-rank ties, hot growing collections, stopped producers,
   backpressure, cancellation and remote restore. Qualification must identify
   exact selected nodes and all retained structures, not just fixture parity.
5. STR-32 owns capacity/replay/Kafka failures and long-duration qualification;
   do not infer those gates from these first runnable slices.

No Arcstream runtime or canonical topic was modified by this audit. Existing
application-reference agents own their fixture/oracle changes.
