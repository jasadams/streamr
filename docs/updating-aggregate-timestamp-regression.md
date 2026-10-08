# Updating aggregate timestamp regression

An updating `GROUP BY` uses a hidden `MAX(_timestamp)` aggregate. The planner appends it after the caller's aggregate expressions, and the resulting timestamp supplies the output row's event time. The legacy worker also used a null result from this last aggregate as its empty-group sentinel: it retracted the previous output without appending a replacement. These are engine behaviors, not a caller-selected clock rule.

This hidden aggregate predates Streamr's native state implementation. The planner addition is in upstream-era commit `af5d2769` (`Nested updating queries`, 2025-02-16), at [`plan/aggregate.rs`](../crates/arroyo-planner/src/plan/aggregate.rs). Debezium unrolling assigns an update's current envelope `_timestamp` to **both** its `before` retraction and `after` insertion; a delete's `before` row also receives the current envelope timestamp. That code dates to upstream-era commit `74d009723`, with the `before` row selection corrected by `0e05c417`; see [`physical.rs`](../crates/arroyo-planner/src/physical.rs). A `before` payload does not supply the timestamp of its earlier insertion.

The older worker chose DataFusion's sliding MAX because it reports retraction support. In pinned DataFusion 48, `SlidingMaxAccumulator::retract_batch` ignores the supplied value and pops the oldest queued value by row count. Its `state()` contains only the current maximum, so checkpoint and restore do not preserve that queue. This avoids an exact-value lookup error when the CDC envelope timestamp changes, but it can yield the wrong maximum after an arbitrary retraction, even without recovery. This is an observed source-code limitation, **not** an intentional upstream contract or proof of pristine upstream runtime behavior.

Streamr's native updating aggregate instead persists indexed MAX members through the configured state backend. On a changelog input, it requires a retraction to match a retained value exactly. A valid CDC update or delete can therefore fail with `native aggregate retracts a nonmatching indexed member`: its `before` row carries the new envelope timestamp, while the indexed member carries the earlier insertion timestamp. The indexed choice was introduced in `aea39170`; the current strict diagnostic is in [`incremental_aggregator.rs`](../crates/arroyo-worker/src/arrow/incremental_aggregator.rs). The new failure is distinct from the older sliding implementation's FIFO and lossy-checkpoint defects.

The planner places the injected MAX last, after any caller-written `MAX(_timestamp)`. A repair can use that final ordinal only after validating the aggregate plan and input expression; it must not change a caller's MAX by matching the function name or output alias alone. Replacing exact matching with durable FIFO would reproduce part of the older behavior, but would not compute MAX over arbitrary surviving rows. Exact retraction needs a proven way to recover the old member's timestamp. The approved repair preserves the current-member MAX rule; it does not introduce a new clock rule or FIFO compatibility policy.

## Runtime baseline (2026-10-05)

The generic 12-change CDC comparison ran at [`target/updating-timestamp-legacy-native-baseline`](../target/updating-timestamp-legacy-native-baseline). Both queries use only `SELECT k, COUNT(*) AS active FROM baseline_input GROUP BY k`. The manifest selects `STREAMR_TEST_NATIVE_AGGREGATES=0` for [`legacy/query.sql`](../target/updating-timestamp-legacy-native-baseline/legacy/query.sql) and `1` for [`native/query.sql`](../target/updating-timestamp-legacy-native-baseline/native/query.sql). It also supplies the external checkpoint-capture environment, input hashes, source batch size 1, checkpoint after eight source rows, and epoch 1. This compares the two operator paths in the current Streamr build; it is **not** an untouched upstream binary comparison.

The independent value oracle requires checkpoint state `a=3, b=2` and final state `b=2` with no `a`. The legacy capture exited 0 in 0.42 seconds and restored the correct checkpoint counts, but both its uninterrupted and recovered final outputs retained `a=0`. Its value comparison therefore failed. The native capture exited 101 in 0.32 seconds with `native aggregate retracts a nonmatching indexed member`, before completing checkpoint/recovery. Neither path passed. Logs, outputs, and the strict value comparison remain in the baseline directory; the comparator reconstructs the expected counts from the exact input before/after images.

The tested worker source SHA-256 was `3d004bec39dba16f5cf3e1b2debd2a5c3fafd337287f251c005c808caf1529e4`; the compiled capture binary SHA-256 was `1a3e2bdc7fee1b542150479703873f202c5e5cd5903a5a0141c42476b2aff1b9`. No production source was changed for this comparison. The same source had passed the combined development gates (627 library tests, four ignored); those checks did not establish CDC value correctness. Simply restoring the older accumulator is not a demonstrated fix.

## Approved repair

Keep the planner's intended MAX over the timestamps of currently contributing rows. For the injected timestamp aggregate, use the existing updating row ID to locate the retained timestamp member, instead of expecting the new change timestamp to equal the old one. An insert of row 7 at time 100 would retain a timestamp member and an ID-to-member index entry. An update of row 7 at time 105 would use ID 7 to remove the member for time 100, then insert the member for time 105. This can replace the existing secondary timestamp-value index; it does not require an event history, new SQL syntax, application callbacks, or per-row database transactions. The configured state backend, admitted write scopes and checkpoint lifecycle remain responsible for the index.

The user approved this repair. The worker implementation replaces the secondary value index with an ID-to-member locator only for a validated engine-injected timestamp MAX. Ordinary caller aggregates retain exact value retraction. The planner scopes updating IDs at each `UNION ALL` input, including shared and nested branches, so equal keys from separate inputs remain distinct. Normal checkpoint recovery uses the persisted logical program and preserves those branch ordinals; migration to a recompiled or reordered program is not qualified. The affected state codec fingerprint changes, so older incompatible checkpoints fail closed.

Source review and the final combined development gates and runtime matrix passed; the failed intermediate attempts remain below. The generic fixture in [`native_updating_timestamp_cdc.py`](../scripts/native_updating_timestamp_cdc.py) contains updates, group moves and deletions, and asserts caller-written `MAX(position)` alongside counts. Its 24 cases cover direct, duplicate UNION and shared/nested CTE queries across memory/RocksDB, controller/leader checkpoint ownership, and source batch sizes 1/8. It checks exact committed and final values through fresh-worker recovery. A prepared fixture is not evidence of a successful execution.

## First approved-repair validation attempt

All five Bookworm development gates passed: formatting, workspace/all-target check, strict Clippy, 635 library tests (four ignored), and workspace/all-target build. The worker SHA-256 was `63c9311ff82ab429bfce84b770857eb1b5f3066b3a8eecc5c79b35936781f12e`; planner `plan/mod.rs` was `81575654bbd55a184ab7699c77256d05f0a33c131b8aaa1d771a66a842fb4cbe`. The SQL capture binary SHA-256 was `8de92f199e10e0fefd741edc54daa708f5fa8c638890f86b3e529fb83172e467`.

At `target/native-updating-timestamp-identities`, the memory/controller/batch-1 direct query completed initial execution and fresh-worker recovery. Its emitted checkpoint had `a=3/MAX=4` and `b=2/MAX=3`; both final states had only `b=1/MAX=3`. The following UNION case failed before execution with `KeyCalculationExtension should have exactly one input`. The combined pipeline stopped there, leaving the remaining cases and compatibility matrices pending. This is partial diagnostic evidence, not a 24-case qualification. The failing query and logs are retained while the planner path is repaired.

## Qualified repair (2026-10-05)

The final run passed formatting, workspace/all-target check, strict Clippy,
636 library tests (four ignored) and workspace/all-target build. The complete
24-case generic matrix passed strict initial, committed checkpoint-8 and
recovered-final comparisons at
`target/native-updating-timestamp-identities-union-fixed/comparisons.json`.
Counts have multiplicity 1/2/4 for direct/UNION/shared-CTE queries; caller-written
MAX(position) retains its exact values; deleted groups disappear.

Each logical UNION now has one consolidation boundary using the existing
stateless value operator. Its physical plan is a single identity memory reader,
so each incoming branch batch contributes once. Each branch still computes its
own updating ID scope. The planner validates branch count and Arrow schema
agreement. A shared CTE referenced twice produces three logical UNION boundaries
after DataFusion inlining; the regression test checks that actual graph.

Worker SHA-256 remains `63c9311ff82ab429bfce84b770857eb1b5f3066b3a8eecc5c79b35936781f12e`.
Planner `plan/mod.rs`: `39e2d51e22628eae8a93b736ffa3e14390c47c18cf6e7ff27d6438d231ef8417`;
`extension/remote_table.rs`: `d088e9d520b2a83c1d773e809d2f584cd8827c2d60344b9c4df83912aa11cc3f`.
SQL capture executable: `35dafbefefc91699a486e686615dbbdc580888e6ff4764ad101a5db75471fa2f`.
The proof directory contains all six compiler-source hashes and gate references
in `source-evidence.json`. The same executable passed eight typed-array, eight
empty-array, eight external already-ranked array and eight external profile-core
captures with strict comparisons, plus the expected oversized-array failure.

This qualifies the selected query shapes and recovery configurations. It does
not qualify recompiled-plan checkpoint migration, general ranking, unrestricted
collection capacity or full milestone 3 acceptance.
