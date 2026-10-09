# Native capability audit for STR-28

This source audit records the milestone 3 worktree at
`d59124ea2702cdd0d425725e9afae6310fd6e5e4` on 2026-10-04, read against
[STR-28](https://trakkt.app/issues/STR-28), including its four substantive compatibility comments and revised
acceptance. It preserves candidate routes and source restrictions from that
snapshot; later implementation and test evidence supersedes its status statements
where noted below. The [support matrix](milestone-3-support-matrix.md) and
[current validation record](milestone-3-validation.md) describe tested routes and
remaining gaps. Source inspection alone is not a physical-plan capture or value
test.

Application SQL sketches remain in the external Arcstream repository at
`deploy/streamr-local/proposed/{identity,profiles,sessions}.sql`, with their status
in `proposed/README.md`. The reference handoffs and 33-field profile/12-field
session payloads were read from that repository's `test/streamr-reference` and
`flink/identity-resolution/src/test/resources/reference` directories. Engine
checks must not require these files. Exact application query/reference revisions,
accepted policies and native graph captures still need pinning before adoption.

## Field and lifecycle mapping

The following references external output names solely as compatibility evidence.
They do not establish platform schemas or application logic. “Candidate” describes
the route proposed in this audit; “difference” means application discussion is
required; “gap” means the selected native path has not demonstrated the required
behavior. See current validation evidence for routes tested since this audit. The
complete profile payload has 33 fields; the grouped rows below cover all 33.

| External profile fields | Native candidate or remaining decision |
| --- | --- |
| `canonical_id`, `tenant_id` | Group keys; tenant/canonical ownership differs from the production canonical-only key and needs an explicit decision. |
| `user_id`, `last_page`, `last_country`, `last_device`, `last_browser` | Ordered `LAST_VALUE` with nonnull/nonempty `FILTER`; generic native ordered-value and recovery probes now pass on memory/RocksDB. The external production payload still lacks a stable replayable arrival sequence, so this does not establish those fields' application semantics. |
| `first_seen` | Ordered `FIRST_VALUE` of accepted time by arrival sequence; generic ordered-value and recovery probes now pass, but the external payload still needs a stable sequence and accepted-time policy. `MIN(event_time)` differs for older arrivals. |
| `last_seen` | `MAX` of accepted time; parsing, wall-clock clamping and older-input handling remain application-owned and must match reference values. |
| `updated_at`, `timestamp` | Emission-clock values, not source timestamps; source-driven expressions do not prove idle emission or measured wall-clock bounds. |
| `total_events`, `page_views`, `clicks`, `logins`, `feature_uses` | Generic native COUNT/conditional SUM/FILTER values and recovery are covered by current probes; application integer widths, NULL/retraction policies and exact external outputs still require comparison. Internal signup counts in the sketch are not an additional external field. |
| `total_sessions` | Current session-ID transitions are required; `COUNT(DISTINCT session_id)` and counting nonempty IDs are different. No equivalent selected native transition plan is frozen. |
| `events_1d`, `events_7d`, `events_30d`, `events_90d` | Generic TUMBLE/HOP scalar and closed-window reaggregation routes have passed selected value/recovery captures. UTC calendar buckets, boundary inclusion, historical replay and zero on quiet-key expiry remain unqualified; a trailing-duration count or closed-window row is not automatically equivalent. |
| `sessions_1d`, `sessions_7d`, `sessions_30d`, `sessions_90d` | Windows over the required transition relation, which is unresolved above. Generic SESSION scalar, bridge and checkpoint routes have passed selected captures, but independently counted SESSION outputs have different assignment/output rules. |
| `avg_session_duration_sec`, `current_session_active`, `current_session_duration_sec` | Requires current/completed-session lifecycle and wall-clock active duration. MIN/MAX spans alone do not supply this contract. Native closure/composition must be evaluated before discussing a new interface. |
| `top_pages`, `top_features` | Separately keyed lifetime counts followed by bounded rank and native array construction; current ranking requires already windowed input. A window changes lifetime retention. Whole-history `ARRAY_AGG` followed by truncation is not bounded evidence. Count ties and feature eligibility are application policies. |
| `action`, `trigger` | Creation/event/timeout/decay emission provenance; generic aggregate CDC rows alone do not establish these fields. |
| `changed_fields` | Compare with the last emitted payload, retaining that payload or equivalent bounded per-field state. Comparing with the previous input/aggregate update differs; native list type, membership and admitted output size must be tested. |

Creation emits immediately; subsequent activity coalesces for five processing-time
seconds. Event-time session timeout and 1/7/30-day decay emit without new input,
including zero counts and correct last-emitted deltas. Aligned TUMBLE/HOP closure
is a different emission policy. Native windows own closure, but the sketch does
not establish this combined schedule or its recovery. Durable timer storage APIs
alone provide neither dispatch nor output/clear atomicity. No public callback or
timer API is selected by this audit.

The user subsequently deferred the optional immediate-first and per-group
coalescing policy to post-MVP STR-44. Milestone 3 retains Arroyo's periodic
aggregate flushing and records that compatibility difference. Quiet expiry,
profile/session values, last-emitted deltas and recovery remain required;
this deferral does not approve another clock or lifecycle policy.

The session table covers all 12 external fields:

| External session fields | Native candidate or remaining decision |
| --- | --- |
| `session_id`, `tenant_id` | SESSION grouping keys; production session-ID-only ownership needs uniqueness proof or an approved tenant-key correction. Reused keys after closure require fresh state. |
| `canonical_id` | First arrival owns identity, even if subsequent canonical IDs differ. Ordered `FIRST_VALUE` requires stable arrival order and passing value tests. Grouping by canonical ID would split this contract. |
| `start_time` | First accepted arrival time; native minimum/event-time start differs. Formatting and exact UTC millisecond values remain required. |
| `end_time` | Maximum accepted event time; native window end adds the inactivity gap and differs. |
| `duration_sec` | End minus first-arrival start, with declared integer conversion; MAX minus MIN differs on older records. |
| `event_count` | Native COUNT candidate over the shared selected assignment; late filtering/gap splitting can change membership. |
| `pages` | Complete distinct native string array with explicit element/byte limits and failure behavior; current scalar sketch omits it. No silent truncation or independent filtered SESSION assignment. |
| `event_types` | Complete native string-to-integer frequency object with bounded entry/byte limits; separate frequency/rank storage alone is not a typed SQL output implementation. |
| `device_type`, `browser`, `country` | Last nonempty arrival; generic ordered LAST_VALUE/FILTER probes pass, while the external session payload still lacks a stable arrival sequence and application-specific parity evidence. |

Native SESSION closes when watermark is strictly greater than last-event-plus-gap
(`session_aggregating_window.rs`, `watermark_update`); the external baseline
closes at equality. It drops input older than the watermark (`process_batch`),
while the baseline accepts older arrivals. Native assignment splits event-time
gaps before timer-driven closure; a named baseline session can remain open across
such a gap. These are concrete differences requiring discussion, not accepted
policy corrections. Test canceled/extended deadlines, equality and the next
instant, supplied-ID reuse, old records and restoration around closure.

`SessionWindowConfig` contains `gap` but no explicit maximum-duration setting.
Its legacy `s` table uses retention `gap * 100`; retention is not a session cap.
Do not infer an accepted 24-hour maximum from the default 24-hour updating-
aggregate TTL. Continuous sessions lasting at least 24 hours require value,
closure, capacity and restore checks; any proposed duration cap needs discussion.

Identity forwards all 17 original/resolved event fields and four directed-merge
fields. Original payload pass-through uses projections. The STR-38–42 route now has
two typed tables under one ordered owner, same-event OLD/source/NEW context and
native UUID allocation; the current validation record reports eight captures and
16 strict initial/recovered comparisons against the application-owned Flink
oracle. This evidence is specific to that pinned fixture. Storage and named
stage-output relations remain distinct; candidate/replay allocation and tenant
policy belong to the application. Ordinary CTEs or independent MERGE change rows
do not supply this contract.

## Planner and configured-backend restrictions

These restrictions are directly visible in source. Actual positive/negative
physical-plan probes and exact failures remain a validation gate.

| Composition | Source restriction / consequence |
| --- | --- |
| Updating aggregate input to joins | `plan/join.rs::check_updating` rejects an updating left or right input. Scalar lifetime aggregates cannot simply join into a combined result. |
| Window joins | `check_join_windowing` rejects windowed/nonwindowed mixtures, unequal window types/parameters and SESSION joins; nonwindowed general outer joins also reject. Different-width rolling results are not freely joinable. Lookup joins use a separate constrained path. |
| Ranking / analytic expressions | `plan/window_fn.rs` requires already windowed input, rejects SESSION, requires exactly one expression and exactly one window field in PARTITION BY. The lifetime rank sketches do not meet that input contract. |
| Nested aggregation | `plan/aggregate.rs` requires matching windows; SESSION cannot be reinvoked in nested aggregates and must carry its window struct. Shared assignment must be proved before filtering collections. |
| RocksDB live SQL | At this audit snapshot, admission was described by a narrow operator allowlist. Since then, native aggregates, TUMBLE/HOP, SESSION and state tables use the common configured-backend construction path; current memory/RocksDB runtime evidence is recorded in the validation document. Ranking and general joins/composition remain path-specific planner restrictions; a memory pass alone is not RocksDB evidence. |

Native aggregate, window, SESSION and state-table owners now share the configured
backend construction adapter (STR-39), with one SQL semantics implementation.
The tested runtime/checkpoint scope and remaining gates are recorded in the
validation document and support matrix. Broad historical joins and analytic
frames remain milestone 4; source restrictions alone do not establish a need for
a new composition contract or move a broad feature into milestone 3.

## Retained ownership and resource inventory

The selected disk SQL contract currently fixes singleton parallelism/subtask zero.
Memory operators may have keyed subtasks; this audit does not qualify repartition
or rescale. All structures below belong to an operator/attempt, including derived
state, pending work and in-flight outputs. Some selected native paths now use
registered checkpointed state and explicit budgets; complete bounds for every
key/value/collection/channel/output allocation are not established.

| Owner / structure | Registration, backend and remaining bound |
| --- | --- |
| Legacy updating aggregator accumulators and `UpdatingCache` eviction links | The `a` accumulator and `b` fallback-row timestamp tables describe the pre-migration owner. The selected native aggregate path and its current limits are summarized in the support matrix and validation record. |
| Updating `updated_keys` and previous values | HashMap retains changed keys plus previous ScalarValue vectors until flush. Outputs contain retract/append rows and suppress equal updates. This previous result is not the external last-emitted profile after coalescing. Flush assembly, iteration and output admission require bounds. TTL eviction emits retracts and can discard lifetime history. |
| Legacy TUMBLE/HOP computation holders | `execs` BTreeMaps, bins/panes, record-batch holders, DataFusion aggregate state, and unbounded internal input channels describe the pre-migration route. The selected paged native path and its capacity limits are in the support matrix and validation record; generic channel-byte bounds remain open. |
| Legacy SESSION computations and start/deadline indexes | `key_computations`, start/deadline indexes, raw batches and legacy `e`/`s` tables describe the pre-migration route. The selected native paged SESSION state and tested hot-session checkpoint shape are summarized in the support matrix and validation record; broader cardinality and channel-byte bounds remain open. |
| Ranking/array composition | Window-function `execs` BTreeMap, pending futures, raw batches, unbounded channels and sort/window executor state; registers legacy `input` timestamp table. Selected plan capacity remains unqualified. Lifetime member counts need separately addressable entries, bounds on active cache, exact tie ordering, rank maintenance and admitted array construction. Existing `RankedCounts` is storage evidence only. |
| Native typed tables and carried event results | STR-38–42 now supplies a tested typed-table/ordered-owner route; every durable namespace/schema/index must register for export/restore. The current state-table captures cover named MERGE effects, lookup values and fresh-worker recovery. Broader OLD/source/NEW buffers, cancellation/output admission remain separate concerns. |
| Generic timers / ranked collections | Primary and deadline/rank indexes stay in one registered live namespace. Prepared buffers, retained pages, scans and composed batches need reservations through commit. One serial owner excludes competing writers; storage atomicity is not output atomicity. |
| Arrow history | Chunk and expiry namespaces are derived. Every namespace must explicitly register in checkpoint metadata; factory construction alone does not register/export it. Appending chunks can commit a prefix. |
| Graph and operator channels | Message counts are bounded for graph queues; byte admission remains STR-16. Legacy window channels were unbounded; the selected native window path uses a one-slot final reader. Graph-wide byte admission, slow-sink behavior and cancellation with retained batches remain to be qualified; stateless executor tests do not establish them. |
| Source offsets | Single-file capture source registers global `f` (file-to-lines-read); filesystem source uses `a`; Kafka source registers global `k`. Partition/file progress and watermark/idleness metadata must match the authoritative selected checkpoint and declared connector ownership. A fixture's finite metadata does not prove arbitrary cardinality bounds. |
| Sink progress / pending commits | Single-file capture sink registers global `f` and restores/truncates to its recorded byte offset. Kafka committing sink registers global `i` transaction-ID index with two-phase commit and retains producer/transaction state. Commit-phase restoration is explicitly unfinished in `kafka/sink/mod.rs::handle_commit`. Delivery requires separate fault qualification. |
| Checkpoint selection and output | Controller/leader selects committed checkpoint; registered tables export schema/ownership/page metadata. Local WAL does not select recovery. Prepared output, previous emitted result, timers and source/sink progress must align; shared DB ownership alone proves none of this. |

The support matrix records concrete existing live-map/read/scan/checkpoint limits.
Execution accounting is cooperative, spilling disabled, and max-batch checking
occurs after output allocation. Arbitrary expression/UDF allocations and legacy
retained structures are not universally admitted. Large values require declared
failure behavior; neither RAM residency nor output size is bounded merely by
placing records on disk.
