# Native capability audit for STR-28

This is a source audit of the milestone 3 worktree based on
`d59124ea2702cdd0d425725e9afae6310fd6e5e4` on 2026-10-04, read against
[STR-28](https://trakkt.app/issues/STR-28), including its four substantive compatibility comments and revised
acceptance. It freezes the candidate routes and demonstrated source restrictions;
it does not freeze a runnable replacement plan or establish native qualification.
The [support matrix](milestone-3-support-matrix.md) and
[validation record](milestone-3-validation.md) retain the implementation and test
status. Source inspection below is not a physical-plan capture or a value test.

Application SQL sketches remain in the external Arcstream repository at
`deploy/streamr-local/proposed/{identity,profiles,sessions}.sql`, with their status
in `proposed/README.md`. The reference handoffs and 33-field profile/12-field
session payloads were read from that repository's `test/streamr-reference` and
`flink/identity-resolution/src/test/resources/reference` directories. Engine
checks must not require these files. Exact application query/reference revisions,
accepted policies and native graph captures still need pinning before adoption.

## Field and lifecycle mapping

The following references external output names solely as compatibility evidence.
They do not establish platform schemas or application logic. “Candidate” means a
native expression to test; “difference” means application discussion is required;
“gap” means the selected native path has not demonstrated the required behavior.
The complete profile payload has 33 fields; the grouped rows below cover all 33.

| External profile fields | Native candidate or remaining decision |
| --- | --- |
| `canonical_id`, `tenant_id` | Group keys; tenant/canonical ownership differs from the production canonical-only key and needs an explicit decision. |
| `user_id`, `last_page`, `last_country`, `last_device`, `last_browser` | Ordered `LAST_VALUE` with nonnull/nonempty `FILTER`; arrival order needs a stable replayable sequence absent from the production payload. Known incorrect ordered values remain a defect until native value tests pass. |
| `first_seen` | Ordered `FIRST_VALUE` of accepted time by arrival sequence; `MIN(event_time)` differs for older arrivals. The ordered-value defect also blocks assuming this path is correct. |
| `last_seen` | `MAX` of accepted time; parsing, wall-clock clamping and older-input handling remain application-owned and must match reference values. |
| `updated_at`, `timestamp` | Emission-clock values, not source timestamps; source-driven expressions do not prove idle emission or measured wall-clock bounds. |
| `total_events`, `page_views`, `clicks`, `logins`, `feature_uses` | Native `COUNT`/conditional `SUM`/`FILTER` candidates; verify integer widths, NULL behavior, retractions and exact outputs. Internal signup counts in the sketch are not an additional external field. |
| `total_sessions` | Current session-ID transitions are required; `COUNT(DISTINCT session_id)` and counting nonempty IDs are different. No equivalent selected native transition plan is frozen. |
| `events_1d`, `events_7d`, `events_30d`, `events_90d` | TUMBLE/HOP count candidates; UTC calendar buckets, boundary inclusion, historical replay and zero on quiet-key expiry require tests. A trailing-duration count or closed-window row is not automatically equivalent. |
| `sessions_1d`, `sessions_7d`, `sessions_30d`, `sessions_90d` | Windows over the required transition relation, which is unresolved above. Independently counted SESSION outputs have different assignment/output rules. |
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
| `device_type`, `browser`, `country` | Last nonempty arrival; ordered LAST_VALUE/FILTER remains blocked by the known defect until tested. |

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
fields. Original payload pass-through uses projections. Resolution and directed
OLD-anonymous/NEW-user comparison require STR-38–42: two typed tables under one
ordered owner, user-first writes, same-event OLD/source/NEW context, match/no-op
pass-through and one result per source event. Ordinary CTEs or independent MERGE
change rows do not supply this contract. The external DuckDB serial experiment
is not native plan/runtime/recovery evidence. Storage and named stage-output
relations must remain distinct; candidate/replay allocation and tenant policy
belong to the application.

## Planner and configured-backend restrictions

These restrictions are directly visible in source. Actual positive/negative
physical-plan probes and exact failures remain a validation gate.

| Composition | Source restriction / consequence |
| --- | --- |
| Updating aggregate input to joins | `plan/join.rs::check_updating` rejects an updating left or right input. Scalar lifetime aggregates cannot simply join into a combined result. |
| Window joins | `check_join_windowing` rejects windowed/nonwindowed mixtures, unequal window types/parameters and SESSION joins; nonwindowed general outer joins also reject. Different-width rolling results are not freely joinable. Lookup joins use a separate constrained path. |
| Ranking / analytic expressions | `plan/window_fn.rs` requires already windowed input, rejects SESSION, requires exactly one expression and exactly one window field in PARTITION BY. The lifetime rank sketches do not meet that input contract. |
| Nested aggregation | `plan/aggregate.rs` requires matching windows; SESSION cannot be reinvoked in nested aggregates and must carry its window struct. Shared assignment must be proved before filtering collections. |
| RocksDB live SQL | `worker/engine.rs` admits only watermark/value/key/projection/stateful-map/source/sink operators and requires every node parallelism one, unchanged on restore. Updating aggregates, TUMBLE/HOP/SESSION, rank and joins remain gated. A memory plan is not RocksDB support. |
| Legacy disk map projection | `arrow/stateful_processor.rs` validates primitive/UTF-8 row types and a scalar allowlist. Lists/maps and arbitrary profile/session UDFs are not qualified by this path. |

STR-17/19/20/29 must migrate through a common configured-backend lifecycle
adapter (STR-39), keeping one SQL semantics implementation. Remove a RocksDB gate
only for the exact path with admitted working-state, runtime and checkpoint
proof. Broad historical joins/analytic frames remain milestone 4; this audit
demonstrates source restrictions but has not established the minimal new
composition contract or authorized moving a broad feature into milestone 3.

## Retained ownership and resource inventory

The selected disk SQL contract currently fixes singleton parallelism/subtask zero.
Memory operators may have keyed subtasks; this audit does not qualify repartition
or rescale. All structures below belong to an operator/attempt, including derived
state, pending work and in-flight outputs. No selected native migration has yet
provided complete key/value/collection/channel/output byte bounds.

| Owner / structure | Registration, backend and remaining bound |
| --- | --- |
| Updating aggregator accumulators and `UpdatingCache` eviction links | Legacy `a` accumulator and `b` fallback-row timestamp tables. Cache contains active accumulators; nonretractable fallback has per-value counts/generations and changed-value sets. No configured live-disk migration or byte-bounded active cache is established. |
| Updating `updated_keys` and previous values | HashMap retains changed keys plus previous ScalarValue vectors until flush. Outputs contain retract/append rows and suppress equal updates. This previous result is not the external last-emitted profile after coalescing. Flush assembly, iteration and output admission require bounds. TTL eviction emits retracts and can discard lifetime history. |
| TUMBLE/HOP computation holders | `execs` BTreeMaps, bins/panes, record-batch holders, DataFusion aggregate state, and unbounded internal input channels. Legacy `t` timestamp table checkpoints state; checkpoint serialization does not bound RAM residency or channel bytes. |
| SESSION computations, start/deadline indexes | `key_computations` HashMap, `keys_by_start_time`/`keys_by_next_watermark_action` BTreeMaps and key sets; raw batches by start time, active final aggregation and unbounded channel. Registers legacy `e` global earliest-start and `s` timestamp tables. Raw-history checkpointing is explicit; partial aggregation is TODO. |
| Ranking/array composition | Window-function `execs` BTreeMap, pending futures, raw batches, unbounded channels and sort/window executor state; registers legacy `input` timestamp table. Selected plan capacity remains unqualified. Lifetime member counts need separately addressable entries, bounds on active cache, exact tie ordering, rank maintenance and admitted array construction. Existing `RankedCounts` is storage evidence only. |
| Native typed tables and carried event results | STR-38–42 planned; every durable namespace/schema/index must register for export/restore. OLD/source/NEW buffers and cancellation/output boundary need admission. No migration from a scalar map checkpoint is assumed. |
| Generic timers / ranked collections | Primary and deadline/rank indexes stay in one registered live namespace. Prepared buffers, retained pages, scans and composed batches need reservations through commit. One serial owner excludes competing writers; storage atomicity is not output atomicity. |
| Arrow history | Chunk and expiry namespaces are derived. Every namespace must explicitly register in checkpoint metadata; factory construction alone does not register/export it. Appending chunks can commit a prefix. |
| Graph and operator channels | Message counts are bounded for graph queues; byte admission remains STR-16. Selected window channels are unbounded. Slow sinks and cancellation must be exercised with retained batches, not inferred from stateless executor tests. |
| Source offsets | Single-file capture source registers global `f` (file-to-lines-read); filesystem source uses `a`; Kafka source registers global `k`. Partition/file progress and watermark/idleness metadata must match the authoritative selected checkpoint and declared connector ownership. A fixture's finite metadata does not prove arbitrary cardinality bounds. |
| Sink progress / pending commits | Single-file capture sink registers global `f` and restores/truncates to its recorded byte offset. Kafka committing sink registers global `i` transaction-ID index with two-phase commit and retains producer/transaction state. Commit-phase restoration is explicitly unfinished in `kafka/sink/mod.rs::handle_commit`. Delivery requires separate fault qualification. |
| Checkpoint selection and output | Controller/leader selects committed checkpoint; registered tables export schema/ownership/page metadata. Local WAL does not select recovery. Prepared output, previous emitted result, timers and source/sink progress must align; shared DB ownership alone proves none of this. |

The support matrix records concrete existing live-map/read/scan/checkpoint limits.
Execution accounting is cooperative, spilling disabled, and max-batch checking
occurs after output allocation. Arbitrary expression/UDF allocations and legacy
retained structures are not universally admitted. Large values require declared
failure behavior; neither RAM residency nor output size is bounded merely by
placing records on disk.

## STR-43 removal inventory

The removal target is the SQL surface `state_get`, `state_put`, `state_upsert`,
`state_update`, `state_delete`. Ordinary backend get/put, table INSERT/MERGE and
generic storage/checkpoint support are not automatically removal targets.

| Surface | Current locations / decision needed |
| --- | --- |
| Active registration and lowering | `arroyo-planner/src/functions.rs`, `plan/mod.rs`, `rewriters.rs`, `extension/stateful_processor.rs`; registration, expression detection, map ownership and guarded CASE lowering must migrate together. |
| Worker dispatch and row evaluation | `arroyo-worker/src/arrow/stateful_processor.rs`, `worker/src/engine.rs`; map construction, ordered operations, allowlist/type checks and worker admission are coupled to old serialized plans. |
| Planner callers | `test/mod.rs` and query fixtures `stateful_processor_{get,put,upsert,delete,multi_ops}.sql`, `error_stateful_processor_{non_literal_map,in_filter}.sql`; migrate equivalent generic native regressions before deletion. |
| Native runtime callers | `arroyo-sql-testing/src/smoke_tests.rs` and `src/test/queries/stateful_processor_{operations,shared_ctes,sequential_ctes,computed_cte,qualified_filter}.sql`; associated inputs and golden outputs also belong to the migration inventory. |
| Serialized physical plans | `api.proto`: `StateOpType`, `StateOperation`, `StatefulProcessorOperator`; `arroyo-datastream/src/logical.rs`: `OperatorName::StatefulProcessor` and its name mapping. Persisted programs include opaque physical expression bytes and old function/operator references. Specify versioned rejection or explicit conversion; do not silently reuse enum tags. |
| Checkpoint formats | Memory scalar maps use legacy global keyed state; disk uses `TableEnum::DiskKeyedMap` (tag 3), `DiskKeyedTableConfig`, subtask/task metadata, `DISK_CHECKPOINT_VERSION` (1), logical page files and registered namespace/schema ownership validation. New typed-table schemas must not silently decode these old bytes. Preserve/reject/migrate with explicit version/ownership rules and restore tests. |
| Historical evidence | `docs/milestone-2-{validation,handoff}.md`, historical review logs and milestone 3 provenance document the prior path. Keep provenance clearly historical; they do not qualify the replacement or require deleting generic disk snapshot storage. |
| External consumers | Application legacy SQL and capture fixtures require application-side inventory/migration; engine search cannot certify all deployed callers. Proposed sketches are not migrated callers. |

Reproduce the engine SQL-token inventory with
`rg -l 'state_(get|put|upsert|update|delete)' --hidden --glob '!.git/**' --glob '!Cargo.lock'`.
Also search operator/protocol names, registered table dispatch, generated plan
bytes and fixture/golden names; SQL-token search alone misses serialized callers.
Do not remove catalog/decoder support before native prerequisites and the explicit
old-plan/checkpoint policy pass.
