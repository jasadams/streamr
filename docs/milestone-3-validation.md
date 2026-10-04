# Milestone 3 implementation and verification

This branch builds on milestone 2 PR #4 at `fbd5179a`. It carries the existing
capture harness originating in PR #5 and adds the first STR-16/29 implementation slice:
shared execution accounting, durable dual-clock timers and paginated ranked
collections. These are generic engine capabilities. Application schemas and
business behavior remain in the consuming application. See
[the support matrix](milestone-3-support-matrix.md) for retained paths and
interface gaps requiring discussion before further implementation.

The revised milestone plan is recorded in the support matrix: native aggregates
(STR-17), TUMBLE/HOP (STR-19), SESSION (STR-20), typed result composition
(STR-29), state tables/MERGE (STR-38–42) and legacy state_* removal (STR-43).
This revision does not qualify those paths or change the historical evidence
below. Application query proposals and oracles remain in the application repo;
they are not embedded engine implementations.

## Fresh combined-source STR-29 evidence (uncommitted batch)

The current batch on `fc347312` adds internal serial-owner point reads for native
aggregate chunks without indexed accumulators, one stable native-window snapshot
per watermark pass, and bounded append-only unordered FIRST/LAST. It also
propagates a nested constructor error instead of accepting it as an empty
result. These are existing-engine implementation repairs; they add no SQL
syntax, public backend method, or application policy.

The Bookworm container logs
`/tmp/streamr-m3-native-first-snapshot-fixed-{fmt,check,clippy,units,build}.log`
record passing formatting, workspace all-target check (16.48 seconds), strict
Clippy (18.85 seconds), 25 library suites (610 passed, four ignored, none
failed), and full workspace build (57.26 seconds). The SQL test executable was
`arroyo_sql_testing-df802a90a8396c4e` (SHA-256
`b38ee120315ed1490d9bf96de23de44f6f6e697be8809c52eab42f07bbe35364`).

On this same source, all 16 native current-result retention captures passed:
eight ten-event retraction cases across memory/RocksDB, source batch targets
1/8 and controller/leader checkpoints, and eight many-window cases across
memory/RocksDB, 64/4,096 groups and controller/leader at batch target 8. The
strict CDC/recovery fixture checks before images, checkpoint-prefix values and
final values, including a recent-count 2-to-1 replacement. Artifacts and the
SHA-256 comparison manifest are at
`target/native-result-retention-snapshot-fixed/`. The selected aggregate
checkpoint inventory has the same 11 keys and 4,708 stored value bytes at 64
and 4,096 groups in all four backend/protocol pairings. This count excludes
window/source/sink state, RocksDB overhead and RSS. The prior 120.24- and
900.27-second RocksDB/controller timeouts remain historical failures; the
4,096-group RocksDB controller and leader cases now completed initial and
recovered captures in 166.55 and 143.94 seconds total, respectively. These are
end-to-end observations, not a controlled CPU speedup measurement. See
[native result composition](native-result-composition.md) and
[checkpoint inventory](native-checkpoint-inventory.md) for the exact scope.

All eight unordered FIRST/LAST captures passed across memory/RocksDB, source
batch targets 1/8 and controller/leader checkpoints at
`target/native-unordered-first-last-fixed/`. This does not qualify every
ordered, FILTER, collection, or retraction variant. Full 33-profile and
12-session gates remain open; the 10× TUMBLE capacity run on this source is
pending. A new 14-field application SQL proposal is stored in the application
repository but has not been executed here. No application-specific definitions
were added to Streamr.

## Current STR-28 evidence gate

### Uncommitted combined native engine batch

The subsequent batch based on `fc347312` adds reviewed SESSION cache/paged
retirement, finalized-window projection support, the existing STR-37 Kafka
recovery implementation, bounded append-only MIN/MAX/ordered FIRST/LAST state,
and admission of the existing native `uuid()` function as a bounded projected
value. No new SQL syntax is introduced by these repairs.

Bookworm formatting, workspace all-target checks (31.78 seconds) and strict
Clippy (34.97 seconds) passed. The first library run stopped at one UUID
negative-test diagnostic mismatch; the planner correctly rejected the volatile
primary key. After independently reviewing the one-line assertion correction,
the retry passed 604 library tests with four explicitly ignored, and the full
workspace build completed. Container logs are
`/tmp/streamr-m3-native-combined-{fmt,check,clippy,units}.log`,
`/tmp/streamr-m3-native-combined-{fmt,units}-retry.log`, and
`/tmp/streamr-m3-native-combined-build.log`.

The fresh executable passed eight closed-HOP-to-updating-aggregate captures
with strict initial/recovered CDC value and before-image assertions across
memory/RocksDB, source batch targets 1/8 and controller/leader checkpoints.
Artifacts are `target/native-window-composition-combined`. This qualifies
ordinary window-result reaggregation, not lifetime/current-result joins or
quiet-key zero emission. The first native UUID runtime probe completed its
checkpoint and restore but its oracle incorrectly compared random IDs from
two independent fresh runs. After independently reviewing that fixture-only
correction, all eight native UUID captures passed across the same backend,
batch and protocol matrix. Both keys are inserted before the checkpoint and
repeated afterward; lookup results, dependent MERGE no-action values and
output branches agree with each run's own committed bindings. Artifacts are
`target/native-uuid-state-current`. This is a generic native UUID proof, not
application identity qualification without an extra candidate input field.
Full combined-source capacity, application parity, packaged faults, backfill
and 24-hour gates remain open.

On this combined executable, the external identity SQL then passed eight
captures and 16 strict initial/recovered business comparisons against the
application-owned Flink oracle: 13 unified events and two directed merges in
every phase, across both backends, batch targets 1/8 and checkpoint protocols.
The application now supplies only its original 16 fields; one native SQL
`uuid()` producer precedes the lookups/MERGEs, replacing the fixture-only
prepared candidate field. Query/preparation changes remain external.
Artifacts are `target/native-identity-uuid-combined` (checkpoint 41, prefix 10,
16 MiB execution budget). Whitespace/fault/capacity variants on this route and
packaged Kafka delivery still need qualification.

The 1,024-row SESSION RocksDB smoke and 65,000-row hot-session captures passed
both checkpoint protocols on this executable. Full pre-EOF retained payload
floor is 532,480,000 bytes, above 10 times the conservative 50 MiB pool sum;
peak whole-child RSS is 366,272,512 bytes (controller) and 358,846,464 bytes
(leader), below 512 MiB. The oracle checks exact count, window and complete
first payload in both initial/recovered outputs, not every raw input payload.
The checkpoint prefix is only 32,500 rows (266,240,000 payload bytes), so this
does not qualify 10x checkpoint/export/restore. Artifacts are
`target/native-session-capacity-combined-{smoke,65000}`. A reviewed generic
`--checkpoint-rows` option permits 64,999 retained rows with one suffix row.
That stronger run exited successfully for both protocols at
`target/native-session-capacity-combined-checkpoint-65000`. The checkpoint
retained-payload floor is 532,471,808 bytes, also above 10 times the 50 MiB
pool sum. Whole-child RSS peaks are 361,672,704 bytes (controller) and
365,682,688 bytes (leader); initial and fresh-worker recovered outputs match
the count/window/full-first-payload oracle. This qualifies that hot-session
checkpoint shape, not high-cardinality sessions, arbitrary raw payload parity,
backfill, packaged Kafka faults or the 24-hour gate.

The subsequent four-owner capture harness batch passed formatting, workspace
all-target checks (37.73 seconds), strict Clippy (39.90 seconds), all 604 library
tests (four ignored, 25 suites) and the full workspace/all-target build
(3 minutes 48 seconds). Logs are
`/tmp/streamr-m3-four-owners-{fmt,check,clippy,units,build}.log` inside the
development container. This compiles the test-only owner-limit overrides;
the native lifetime/latest-window UNION value/recovery matrix is separate.

The generic UNION nullability repair passed its branch-order/non-null control
regression and all 605 library tests, strict Clippy, workspace checks and build.
After a test-only 4 MiB scan-page override for four owners, the final combined
batch again passed formatting, checks (4.38 seconds), Clippy (2.19 seconds),
605 tests (four ignored) and full build (8.86 seconds). Logs are
`/tmp/streamr-m3-composition-scan-pages-{fmt,check,clippy,units,build}.log`.
Four memory composition captures passed on the preceding executable; the
subsequent run encountered a valid extra initial CDC update and failed its
exact row-count assumption before RocksDB cases ran. See
`docs/native-result-composition.md`; full composition qualification remains
pending. The read-only checkpoint inspector passed synthetic corruption checks
and inspected all four actual memory prefix checkpoints successfully, without
establishing many-window retention or application emission parity.

### Uncommitted checkpoint/window/lineage batch

The combined batch based on `fc347312` passed Bookworm formatting, workspace
all-target checks (56.50 seconds), and strict Clippy (64 seconds). Its first
workspace library run failed four Kafka connector tests: the isolated
`streamr-m3-test-kafka` broker had exited with a JVM SIGBUS and admin requests
timed out. Restarting that test broker restored topic-list access. The retry
then failed two new planner timestamp-lineage tests (an unavailable source
column and an incorrect assumed output layout). Neither failed run establishes
full-batch acceptance.

After independently reviewed fixture corrections and direct qualified-schema
coverage for middle/appended timestamp layouts, the final combined run exited
zero: formatting, all-target checks (31.38 seconds), strict Clippy (34.07
seconds), 582 library tests (four ignored), and full workspace build (61
seconds). Logs in the development container are
`/tmp/streamr-m3-perf-metrics-lineage-{fmt,check-retry,clippy-retry,units-final,build-final}.log`.

The fresh `arroyo_sql_testing-df802a90a8396c4e` then ran an externally supplied
native identity query with memory state, controller checkpoints, source batches
of eight and 16 MiB execution accounting. Initial and checkpoint-41 recovered
captures each contained 13 rows; the application-owned adapter/oracle compared
all 13 unified events and two directed identity merges successfully in both
phases. Artifacts are `target/native-identity-current`, with runtime log
`/tmp/streamr-native-identity-current-retry.log` in the development container.
This is one backend/protocol/batch combination, not full identity qualification
or profile/window composition evidence. Other runtime matrices, capacity runs
on this final source, and milestone acceptance gates remain open.

The expanded external native identity matrix then passed eight captures and
16 strict business comparisons: memory/RocksDB × source batches 1/8 ×
controller/leader, each with initial and checkpoint-41 recovered output. Every
phase matched 13 unified events and two directed identity merges through the
application-owned oracle. Artifacts are `target/native-identity-matrix-lineage`.
Whitespace, injected mutation failures, Kafka/process faults and capacity are
not covered by this 13-event matrix.

A separate whitespace variant then passed eight captures and 16 strict
initial/recovered comparisons on this same earlier source. It replaces every
`alice` user ID with a space, tab and space, and applies the same bijective rename to the
baseline reference expectations. This checks nonempty whitespace handling,
not a new execution of the Flink oracle. Artifacts are
`target/native-identity-matrix-whitespace`; later combined-source qualification
is still required.

On the same source, the scalar, ordered and collection/UNNEST window scripts
passed all 48 initial/recovered capture cases across both configured backends
and checkpoint protocols; the oversized collection case rejected its output
before final execution as expected. Logs are
`/tmp/streamr-m3-first-partial-{windows,ordered,collections,oversize}.log` in the
development container, with matching `target/native-*first-partial` artifacts.
The ordered fixture requests 128-row source batching and checkpoints 70 input
rows; these logs do not observe the actual operator batch length. Full capacity
and session qualification on subsequent source changes remain separate gates.

### Current foundation batch

The foundation batch committed at `ad90cee9` on top of `d59124ea` passed these
Bookworm library runs through the shared machine build queue:

```sh
/home/jason/repos/streamr/scripts/rust-build podman exec \
  -e CARGO_TARGET_DIR=/app/target/milestone2-runtime \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  streamr-state-build cargo test --locked -j4 -p arroyo-state --lib

/home/jason/repos/streamr/scripts/rust-build podman exec \
  -e CARGO_TARGET_DIR=/app/target/milestone2-runtime \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  streamr-state-build cargo test --locked -j4 -p arroyo-worker --lib
```

The state run passed 68 tests, including memory/RocksDB/third-adapter typed-table
conformance, allocation guards, exact resource admission, and real Parquet
metadata preservation and legacy rejection across both compaction partition
paths. The worker run passed 73 tests, including ordered values, FILTER and NULL
inputs, retractions, duplicate ordering keys, serialized-row reload into a fresh
operator, and state-table preflight rejection before task construction.

The planner run subsequently passed all 113 library tests using the same queue
and container command with `-p arroyo-planner --lib`. This covers retained-table
declarations, component-safe names, named MERGE effects and shared capture,
complete-key INNER/LEFT lookups, deterministic key expressions, related owner
compatibility and native aggregate/window plan probes.

These passes establish the tested foundation, planning and aggregate correctness
slices. Workspace all-target checking and strict Clippy passed for this batch
(`cargo check --locked -j4 --workspace --all-targets` and `cargo clippy --locked
-j4 --workspace --all-targets -- -D warnings`, exit 0). Logs are in the container
at `/tmp/streamr-m3-workspace-{check,clippy}.log`. Formatting and staged whitespace
checks also passed. The full workspace build subsequently passed at `ad90cee9`
(`cargo build --locked -j4 --workspace`, exit 0, 23m 23s; container log
`/tmp/streamr-m3-workspace-build.log`). STR-41 integration starts after this
source-frozen build; its subsequent edits need separate verification.
All seven reported PR checks passed on full head
`ad90cee99a99bdb78fcd4352eefe6c9e47d13645`; the head was rechecked after the CI
watch exited 0. The [CI run](https://github.com/jasadams/streamr/actions/runs/37159026844)
passed its build and strict Clippy steps, 521 tests with four skipped, and all
12 integration tests across four runs. Its local log is
`/tmp/streamr-m3-ci-ad90.log`. This is foundation evidence, not acceptance of
the subsequent STR-41 integration edits.
At that foundation head, STR-40 planning deliberately rejected startup pending
STR-41's fused serial execution, exercised in the subsequent batch below.

### Native state-table execution and recovery

The subsequent STR-41 integration batch builds on foundation `ad90cee9`.
Its latest combined Bookworm library run passed 332 tests: planner 124,
state 69, protocol 59 and worker 80 (container log
`/tmp/streamr-m3-native-runtime-units.log`). Worker regressions invoke the actual
fused owner's `process_batch`: a single row emits without further input/control,
a three-row hot-key/null-key batch splits under a one-row pending budget,
an event budget error releases its scope, and a blocked collector keeps the
decoded pool charged until cancellation releases its transient permits.

The SQL-testing executable rebuilt successfully with `CARGO_INCREMENTAL=0`:
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832`.
The reproducible generic driver is [scripts/test-native-state-tables.py](../scripts/test-native-state-tables.py).
Run it inside the Bookworm container through the shared machine queue:

```sh
python3 /app/scripts/test-native-state-tables.py \
  /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832
python3 /app/scripts/test-native-state-tables.py \
  /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832 \
  --directory /app/target/native-state-table-timestamp-fixtures --target-timestamp
```

Both suites passed on memory/RocksDB, source batch targets 1/8 and controller/
leader checkpoint publication: 16 runs and 64 complete JSON output comparisons.
Each uses 15 generic events, two dependent named MERGEs, a current-row LEFT
lookup, an intermediate sink and two final consumers. Assertions cover repeated
and interleaved composite keys, Unicode/empty key components, NULL keys,
matched/absent no-actions, delete/reinsert and a deliberately invalid arithmetic
expression in an unselected clause. A checkpoint after four events at epoch 1
is published, workers are recreated, and initial/recovered/mirror/intermediate
outputs match independent value oracles. The second suite additionally stores a
BIGINT value named `_timestamp` and selects it as `stored_marker`; event time
retains its separate identity. Unaliased retained/computed `_timestamp` output
is explicitly rejected in the current subset, as documented in
[the SQL contract](state-tables.md).

Artifacts are under `target/native-state-table{,-timestamp}-fixtures`; logs are
in the container at `/tmp/streamr-native-state-table{,-timestamp}-fixtures-*.log`.
Batch sizes 1/8 are source configuration targets; the direct callback regression
independently exercises an actual three-row RecordBatch. These small captures
do not establish beyond-RAM behavior, all fault points, backend switching, or
STR-32's backfill/live gates. The integration source is committed at `67d9c96a`.
Its full workspace build passed (3m 10s, container log
`/tmp/streamr-m3-final-build.log`), and strict workspace all-target Clippy
passed (1m 02s, `/tmp/streamr-m3-final-clippy.log`). Both 16-case capture suites
above were rerun successfully after the final timestamp fix. All seven reported
PR checks passed on full head `94f6228e87ea98f213c17973cbbb9433eb3033b8`;
the head was rechecked after CI completed. The
[CI run](https://github.com/jasadams/streamr/actions/runs/37167544579)
covers this committed integration source: 543 tests passed with four skipped,
and all 12 integration tests passed across four runs. Subsequent native aggregate and
capacity/recovery additions are separate work and require their own gates.
The workspace all-target check and strict Clippy passed for this integration
batch. The workspace library run initially failed six Kafka/MQTT connector
tests because local brokers were absent. With dedicated Kafka 3.9.2 and
Mosquitto test brokers running in the build container's network, its unchanged
workspace connector executable passed all 60 tests (container log
`/tmp/streamr-m3-final-connectors.log`). The final timestamp
projection regression also passes the planner's 125-test library suite.

### State-table retained-capacity and selected-checkpoint recovery

The generic driver [scripts/test-state-table-capacity.py](../scripts/test-state-table-capacity.py)
passed with the SQL executable built from `67d9c96a`, using RocksDB and both
controller and leader checkpoint publication. Each run inserts 65,000 keys with
8,192 bytes of deterministic, varied payload per key, then probes 64 keys spread
across the retained set. Retained payload totals 532,480,000 bytes, more than ten
times the conservative 48 MiB sum of the fixture's executor/live resource limits.
The process RSS envelope is separately declared as 512 MiB.

Controller peak RSS was 296,431,616 bytes; leader peak RSS was 292,675,584 bytes,
measured with Linux `wait4` for the complete worker process. Both runs compare
all 65,064 output rows before and after fresh-worker restore, including old/new
values, actions and current lookup payload equality. The selected checkpoint is
epoch 1 after 32,500 input rows. Four complete comparisons passed in total.
Artifacts and measurements are under `target/state-table-capacity-varied`.
A 100-key smoke run also passed both modes and explicitly did not meet the
ten-times-pool threshold.

This measures logical retained payload and actual process RSS. It does not claim
that physical compressed storage exceeds host RAM, or qualify native aggregates,
windows, backend switching, every failure point, or the full backfill/live gates.
The separate backend-switch checkpoint unit test subsequently passed as part
of the 70-test state library suite. It exercises all four source/destination
backend pairs across full, updated/deleted and empty epochs, incomplete restore
rejection and fresh-attempt retry. Integration build/test gates passed as recorded
below. The subsequent cleanup fix at `4ffac3ff` passed all seven CI checks,
as recorded below.

### Configured native updating-aggregate state

The subsequent STR-17 batch adds `worker.aggregate-state` limits and uses the
configured generic live backend for group accumulators, counted extrema,
ordered members, dirty output state and finite-retention expiry. Memory and
RocksDB use the same SQL/operator code. `SET updating_ttl = NULL` explicitly
requests indefinite aggregate retention; default and finite retention remain
compatible, and join retention remains finite. Ordinary admitted backend batches
and checkpoint barriers provide visibility and recovery; no per-event durable
transaction or synchronous flush is added.

The complete Bookworm workspace library run passed 561 tests, including planner
132, state 70, protocol 59 and worker 90. Formatting, strict workspace all-target
Clippy (36.54s), and the full workspace build (2m 31s) passed. Logs are
`/tmp/streamr-m3-native-aggregate-{fmt,clippy,build,final-units}.log`.
The previous all-target compile check also passed (1m 42s); its log is
`/tmp/streamr-m3-native-aggregate-check.log`.

The generic [native aggregate driver](../scripts/test-native-aggregates.py)
passed eight configured memory/RocksDB × source batch target 1/8 × controller/
leader cases. Its actual SQL composes a per-item MAX aggregation with a second
COUNT/FILTER/SUM/MIN/MAX/ordered FIRST IGNORE NULLS/LAST aggregation, so the second
stage consumes genuine updating-input retractions. It compares full initial,
checkpoint-prefix and recovered values and checks each Debezium before/after
transition. A four-input-row epoch-1 checkpoint precedes fresh-worker restore.
The recovered sink contains two creates and two updates; each update encodes its
before and after rows in one Debezium record.

The optional `--baseline` comparison reproduces an existing legacy moving-MAX
defect after restore/retractions: one group reports 7 instead of the correct 9.
The native counted index returns 9. That negative comparison is retained as
evidence, rather than used as the native value oracle. SQL with equal ORDER BY
tuples also permits ambiguous LAST results; the existing operator demonstrated
different initial and recovered tie choices. Exact native value assertions use
an explicit total-order tie-breaker.

The final-source rerun passed all eight native cases, followed by both generic
state-table suites (16 cases and 64 full comparisons). The reproducible
[ordering diagnostic](../scripts/test-native-aggregate-ordering.py) also passed
four native/legacy memory cases with `--total-order` at source batch targets
1/8, comparing exact initial, checkpoint and recovered values. Its default
equal-order mode records permitted tie differences explicitly rather than
requiring the existing operator's inconsistent choices. Logs are
`/tmp/streamr-m3-native-aggregate-{final-matrix,total-order,equal-order}.log`
and `/tmp/streamr-m3-native-tables-aggregate-{batch,timestamp}.log`.

These small operator captures and units do not qualify aggregate hot-key/high-
cardinality capacity, every failure point, rescaling, native windows or the full
backfill/live gates. The subsequent cleanup fix at `4ffac3ff` passed all seven
CI checks, as recorded below.

### Checkpoint cleanup and live RocksDB directory races

At `4ffac3ff90a94bedb179dd38256e0f7a512bdc96`, all seven PR checks passed,
including both Rust jobs. The exact head was rechecked after the CI watch exited
0. [Rust CI](https://github.com/jasadams/streamr/actions/runs/37175698196)
passed after the backend-switch test awaited its cleanup executor before process
exit. One Rust job on the preceding `aea39170` head had passed the test but then
aborted during native cleanup; that preceding head was not fully green.

Live RocksDB capacity scanning now tolerates child files disappearing during
compaction, while a missing database root and other I/O failures remain errors.
The final local workspace library run for this fix and the scalar/ordered native
window draft passed 566 tests. Five separate processes each passed the exact
backend-switch checkpoint test and exited 0. These results address cleanup and
mutable-directory races; they do not complete the operator fault or capacity
matrices.

The native fixed-window draft passed 20 scalar and 20 ordered SQL captures:
TUMBLE/HOP, configured memory/RocksDB, both checkpoint protocols and variable
source batching, plus legacy memory comparison cases. Each capture checks full
initial and fresh-worker recovered values. The ordered fixture uses 78 rows and
checkpoints after 70; actual debug traces show 70 retained rows split into two
partial chunks. Both existing `hourly_by_event_type` and `sliding_window_end`
goldens passed on each backend. Artifacts are `target/native-windows-final`,
`target/native-windows-ordered-final` and container logs
`/tmp/streamr-m3-native-window-{final,ordered-final}-matrix.log`.

These window results precede the subsequent collection, SESSION and centralized
backend-construction edits. The combined edits passed formatting, workspace
all-target checking, strict Clippy and 572 workspace library tests; their runtime
SQL and full build qualification is separate and still in progress. STR-19 and
STR-20 remain In Progress, with quiet-key, late/idleness, capacity, backpressure
and fault acceptance still open.

### Combined native windows, SESSION and backend construction

The reviewed combined draft uses one configured backend construction adapter
for aggregate, fixed-window, session and state-table owners. The final collection
repair passed `cargo fmt --all -- --check`, workspace all-target checking, strict
all-target Clippy, all 573 workspace library tests (four ignored), and a full
workspace build. Container logs are
`/tmp/streamr-m3-window-collection-repair-{fmt,check,clippy,units,build}.log`.
The fresh SQL executable is
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-df802a90a8396c4e`.

The [collection driver](../scripts/test-native-window-collections.py) passed 16
memory/RocksDB × controller/leader × source batch target 1/128 captures of
ARRAY_AGG, ARRAY_AGG(DISTINCT), COUNT(DISTINCT) and single-column UNNEST. Full
initial/recovered multisets preserve duplicates and NULL values. The 4096-row
negative case rejected the oversized array before final aggregation, rather
than truncating it. Final output admission counts logical flat payload bytes;
working admission still counts decoded array allocations and aggregate state.
Artifacts: `target/native-window-collections-repaired`; container log
`/tmp/streamr-m3-native-window-collections-repaired.log`.

The [SESSION driver](../scripts/test-native-sessions.py) passed eight delayed-
watermark cases across both backends, protocols and source batch targets 1/8.
It compares three complete session rows, including equal-gap joins and an
out-of-order bridge, before and after an open-session checkpoint and fresh-worker
restore. Its `--late-input` mode passed four direct-watermark cases at batch
target 1: four rows below the advanced watermark are dropped and four exact
singleton session rows remain. Watermark progression occurs between delivered
batches; this late-drop oracle is intentionally not asserted for batch target 8.
The default bridge test retains the same three-row oracle across batch targets.
Artifacts: `target/native-sessions-{delayed,late}-combined`; logs
`/tmp/streamr-m3-native-sessions-{delayed,late}-combined.log`.

On this same final collection source, both the scalar and ordered window
matrices passed their 16 native memory/RocksDB × protocol × batch cases again.
Logs are `/tmp/streamr-m3-native-window-collection-source.log` and
`/tmp/streamr-m3-native-window-ordered-collection-source.log`. The earlier
legacy comparison and existing golden passes are recorded separately above.

The [legacy equality driver](../scripts/test-legacy-session-equality.py) passed
all four batch target 1/128 × controller/leader cases. Input times 0, 10, 12
with gap 10 produce one `[0,22)` window with count 3 initially and after restore.
This verifies the sorted-prefix/end-update repair against the existing native
SESSION equality rule; it does not change an external application's lifecycle.

After backend construction was centralized, native aggregates passed all eight
value/recovery cases again. Both state-table suites passed their combined 16
cases and 64 full output comparisons. Logs are
`/tmp/streamr-m3-native-aggregate-factory-combined.log` and
`/tmp/streamr-m3-native-tables-factory{,-timestamp}-combined.log`.

These captures do not establish larger-than-budget window/session state,
cancellation/backpressure, idleness, session collections, current rolling zero
output or the full fault/live gates. UNNEST here projects one flat column; it
does not qualify unrestricted expansion of companion columns. The actual
STR-29 composition planner rejections and complete SQL are saved in
[native result composition](native-result-composition.md). Subsequent CI must
validate the committed combined head separately from the preceding `4ffac3ff`.

### Native window/session capacity fixture and streaming capture

The capture harness now validates one JSON object at a time instead of retaining
an entire output file. This avoids counting a 500 MiB capture file as retained
engine state during RSS qualification. The reviewed helper passed formatting,
workspace all-target checking, strict Clippy and all 573 library tests. Logs:
`/tmp/streamr-m3-streaming-capture-{fmt,check,clippy,units}.log`.

The [capacity driver](../scripts/test-native-window-capacity.py) passed eight
small 8-row TUMBLE/SESSION × memory/RocksDB × controller/leader captures using
8 KiB varied payloads and full checkpoints. These are fixture smoke tests,
not larger-than-budget evidence. TUMBLE compares every key and full payload;
SESSION compares its one emitted count/window/FIRST payload and does not expose
every intermediate retained payload.

At 65,000 rows, the proposed full open-state payload is 532,480,000 bytes
(507.8 MiB), exceeding ten times the conservative 50 MiB fixture pool sum. The
halfway checkpoint contains only 266,240,000 payload bytes (253.9 MiB); that
checkpoint must not be described as 10×. These figures describe fixture admission goals, not a passed capacity result.
Physical compressed disk size is a separate quantity.


### Window snapshot reuse: correctness passed, capacity recovery still open

The working-tree window repair on top of `fc347312` reuses one stable snapshot
for a window's group traversal and one for an expiry traversal, rather than
creating physical RocksDB checkpoint snapshots repeatedly within those loops.
Expiry writes remain bounded and recheck the live catalogue to preserve groups
with future panes. Independent review found no blocker. Formatting, workspace
all-target checking, strict Clippy, all 574 library tests and the workspace build
passed; container logs are `/tmp/streamr-m3-window-closure-{fmt,check,clippy,units,build}.log`.

The repaired binary passed 16 scalar, 16 ordered and 16 collection/UNNEST
captures, with exact initial and recovered outputs across memory/RocksDB and
controller/leader checkpoint modes. The oversized collection was rejected before
final execution as expected. Artifacts are `target/native-windows-snapshot-reuse`,
`target/native-window-ordered-snapshot-reuse` and
`target/native-window-collections-snapshot-reuse`.

The full 65,000-row RocksDB/controller TUMBLE run at
`target/native-window-capacity-reused-snapshot` timed out after 900 seconds during
recovery. Its uninterrupted initial output separately passed the complete
65,000-key, full-payload oracle; the interrupted recovered output contained
49,258 rows. This is **not** a passed recovery, RSS or full capacity gate. A live
observation of peak RSS was below 512 MiB, but no completed child measurement was
returned. The earlier full attempt was deliberately terminated because repeated
snapshot creation made little progress. Neither failed attempt establishes the
milestone's capacity acceptance.

The subsequent 1,800-second-per-capture retry passed both controller and leader
RocksDB TUMBLE modes. Each compared all 65,000 keys and complete 8 KiB payloads
in both uninterrupted and recovered output. Full retained payload was
532,480,000 bytes, above 10× the conservative 50 MiB pool sum; the halfway
checkpoint was only 266,240,000 payload bytes and is **not** a 10× checkpoint.
Whole-child peak RSS was 361,066,496 bytes (controller) and 359,043,072 bytes
(leader), both below the declared 512 MiB limit. Capture elapsed times were
876.81 and 1,020.59 seconds respectively; the timeout was extended without
changing counts, values or memory assertions. Artifacts and measurements:
`target/native-window-capacity-snapshot-reuse-1800/measurements.json` and each
case's `runtime.log`, `output.initial.jsonl` and `output.jsonl`.

This qualification used `fc347312` plus the first snapshot-reuse/expiry repair,
before the later first-partial reuse and checkpoint metrics changes. The latter
changes need their own combined checks and runtime qualification. SESSION
capacity, broader failure points and representative backfill/live gates remain
open.

### Native aggregate value and recovery capture

The SQL-testing binary rebuilt from this batch with `cargo test --locked -j4
-p arroyo-sql-testing --no-run` (exit 0; container log
`/tmp/streamr-m3-sql-build.log`). The selected executable is
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-b1c805b1e01cc8a7`.
Two isolated `external_sql_checkpoint_capture` runs passed with configured
`memory`, 16 MiB execution resources, checkpoint epoch 1 after two of three input
rows, and controller and leader checkpoint publication respectively. All four
complete initial/recovered JSONL comparisons passed.

The generic query under `target/native-aggregate-values/{controller,leader}` uses
these aggregate expressions grouped by `item_key`:

```sql
COUNT(*) AS total,
COUNT(*) FILTER (WHERE selected) AS selected_total,
FIRST_VALUE(item_value ORDER BY ordinal) AS first_value,
LAST_VALUE(item_value ORDER BY ordinal) AS last_value,
FIRST_VALUE(item_value ORDER BY ordinal DESC) AS descending_first,
LAST_VALUE(item_value ORDER BY ordinal) FILTER (WHERE selected) AS selected_last
```

Input `(ordinal, item_value, selected)` is `(1, 'u', true)`, `(2, 'v', false)`,
`(3, 'w', true)`, all under key `x`. The uninterrupted capture emits one create:
`(total, selected_total, first_value, last_value, descending_first, selected_last)
= (3, 2, 'u', 'w', 'w', 'w')`. Recovery preserves the checkpoint's create at
`(2, 1, 'u', 'v', 'v', 'u')`, followed by an update with that exact before-image
and the uninterrupted final after-image. The explicit null before-image is also
compared. Logs: `/tmp/streamr-native-aggregate-{controller,leader}.log`.

This exercises the native planner, operator, ordinary Arrow checkpoint tables,
publication, source/sink progress and recreated worker. The aggregate caches
remain in memory; STR-17's configured live-backend migration, bounded fallback
history and resource qualification remain open. It does not qualify native MERGE.

The rebuilt binary also passed the legacy capture guard and all nine earlier
legacy stateful/stateless capture cases, with 18 complete initial/recovered
payload comparisons. Command: the shared queue runs `python3
/app/target/boundary-capture/run-current.py` with the executable above. This fresh
regression replaces the warm-binary limitation for these legacy cases only.

The [native capability audit](milestone-3-native-capabilities.md) records the
2026-10-04 source review against the full STR-28 ticket and all four substantive compatibility comments.
The revised application sketches, reference payload fields, planner guards,
worker admission, operator retained state, connector records and legacy plan/
checkpoint surfaces were inspected. No Cargo command or native physical-plan/
value/recovery capture was run for this documentation slice. The coordinating
agent owns the combined Bookworm validation queue; pending results are not passes.

The coordinating agent recorded this legacy harness regression command, exit 0:

```sh
/home/jason/repos/streamr/scripts/rust-build podman exec streamr-state-build \
  python3 /app/target/boundary-capture/run.py
```

It reused the warm Bookworm binary selected by
`/tmp/streamr-boundary-sql-build.log`, with the same direct `prost` dependency fix
recorded at `d59124ea`; it predates the new planner/state edits. This is not an
exact-current-head build. The guard test passed once; nine capture tests passed
(legacy stateful cutoffs 4/epoch 7 and 9/epoch 11, stateless cutoff 4/epoch 1,
each in memory/controller/leader modes). All 18 initial/recovered complete payload
comparisons passed. Container logs are
`/tmp/streamr-boundary-{stateful,stateless}-{memory,controller,leader}-{4,9}.log`
for the selected cases; generated payloads are under
`target/boundary-capture/{kind}/{mode}`. These outputs are local build artifacts,
not committed fixtures. This validates the legacy scalar-map/stateless capture
harness only; it does not validate native state tables, aggregates or windows.

Remaining acceptance requires pinned application query/oracle revisions, actual
positive/negative native plan captures, exact ordered aggregate/FILTER/NULL and
collection values, approved lifecycle/ownership differences, configured-backend
migration with registered namespaces and admitted buffers, and restore/output
checks in memory/controller, RocksDB/controller and RocksDB/leader modes.
FIRST_VALUE/LAST_VALUE's reported input/order/filter defects are now covered by
the unit and memory-configured native recovery captures above; their live-backend
migration and broader acceptance remain open. Lifetime ranks, updating-input joins,
unequal-window composition and session ranking/join restrictions have source
evidence but still require actual plan probes and a discussed minimum contract
for any required gap. No public callback API is approved by this audit.

SESSION equality/next-instant deadlines, canceled/extended deadlines, old
arrivals, pre-watermark gap splitting, reused IDs, >=24-hour continuous activity,
quiet producers and restore around closure remain open. Idle output, calendar
expiry-to-zero, processing-time coalescing and last-emitted changed-field deltas
are not demonstrated by scalar sketches or timer storage tests. Old SQL callers
and serialized plans/checkpoints need an explicit removal/migration policy before
STR-43. Source audit is a partial STR-28 milestone, not STR-28 or M3 completion.

### Current composition and retention status

The bounded-capture source passed formatting, workspace all-target checking
(5.52 seconds), strict Clippy (2.34 seconds), 606 library tests with four
ignored across 25 suites, and full workspace build (9.59 seconds). Logs are
`/tmp/streamr-m3-bounded-capture-{fmt,check,clippy,units,build}.log`. On the
subsequent composition-queue batch, formatting, checking (2.58 seconds) and
Clippy (2.15 seconds) passed. Its concurrent library run failed one Kafka
metadata-source test after a timeout; the serial rerun passed all 606 tests
(four ignored), including all 67 connector tests. The SQL-testing smoke suite
within that run passed 43 tests with four ignored in 113.34 seconds. The full
workspace build passed in 8.06 seconds. Logs are
`/tmp/streamr-m3-composition-queue-{fmt,check,clippy,units,units-serial,build-serial}.log`.

The prior 32 MiB RocksDB queued-write attempt reached runtime: the job and all
operators started, then task 17 failed queued-write admission with
`ResourceExhausted` (`queued_write_bytes=33554432`) shortly after startup and
before initial capture/checkpoint. The three aggregate owners need five 2 MiB buffers
each and the window owner needs five 0.5 MiB buffers: 32.5 MiB before metadata.
A reviewed 64 MiB queue
override is confined to both generic test fixtures; production limits and source
guards are unchanged.

The current-result UNION capture passed all eight memory/RocksDB × source batch
target 1/8 × controller/leader cases with strict CDC, before-image, checkpoint-
prefix and final-value comparisons. The prefix has lifetime count 1 and recent
count NULL; final materialized values are lifetime count 3 and recent count 1.
Artifacts are `target/native-result-composition-queue-qualified`; log:
`/tmp/streamr-m3-composition-queue-runtime.log`.

A separate four-event retention probe passed memory/batch-1 controller and
leader checkpoint inspection: each of three owners had `G=1`, and the outer
aggregate had `M=4,R=4`. Its batch-8 companion expected recent count 2 but
observed NULL: the watermark generator takes the minimum event time in each
source batch, including the batch values `[1,3,5]`. That expectation is invalid
for this batch shape and does not demonstrate an engine defect.

The earlier retention qualification attempt contained 16 scopes: eight
ten-event retraction captures (memory/RocksDB × batch target 1/8 × controller/leader) and eight
many-window captures (memory/RocksDB × 64/4096 groups × controller/leader at
batch target 8). Fourteen scopes passed on that earlier source: all eight
retraction cases, all four many-window memory cases, and both 64-group RocksDB
cases. Their artifacts are
under `target/native-result-retention-tail-qualified`; the two then remaining
scopes were the 4096-group RocksDB controller and leader cases.

The checkpoint inventories for the 64- and 4096-group memory cases match across
cardinality and both checkpoint protocols. The three owners each have `G=1`;
the latest result stores 1,840 bytes, the lifetime aggregate 1,456 bytes, and
the outer aggregate 1,200 bytes plus `M=4` entries of 9 bytes and `R=4` entries
of 44 bytes. Total logical value bytes are 4,708 in each checkpoint. This
qualifies the measured many-window fixture's constant checkpoint shape across
64 and 4096 groups; it does not qualify arbitrary retention workloads.

An earlier 4096-group memory capture had prefix recent count 1 rather than 2;
that observation's cause was not established. The revised tail fixture asserts
latest count 2 across last-batch partitions of lengths 1–8, and both 4096-group
memory protocol cases now pass.

The first 4096-group RocksDB/controller attempt ended at the default 120.24-second
runtime timeout before initial capture finished. Its log is preserved at
`target/native-result-retention-tail-qualified/many-4096-rocksdb-8-controller/capture.log`;
the source task ended at 1.6 seconds and the lifetime aggregate at 55 seconds,
which is not a full capture pass. A second controller attempt used an explicit
900-second timeout in `target/native-result-retention-long-rocksdb` and failed
after 900.27 seconds with `worker runtime timed out` before any checkpoint. The
source task ended at 1.6 seconds and the lifetime aggregate at about 30 seconds;
the window, latest-result and outer aggregate tasks did not finish. Its incomplete
log is preserved at
`target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller/capture.log`.
The serial runner uses `set -e`, so the leader case did not run. The previous 120
and 900 second attempts are failures, not completed captures or performance
passes.

The historical controller invocation inside `streamr-state-build` is reproduced
here; it failed as described above:

```sh
STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=900 python3 /app/scripts/test-native-result-retention.py \
  /app/target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller \
  --scenario many --groups 4096 \
  --binary /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-df802a90a8396c4e \
  --backend rocksdb --batch 8 --mode controller \
  --checkpoint-root /app/target/native-result-checkpoints \
  --inspector /app/scripts/checkpoint_inventory.py
```

At that historical source, the 4096-group RocksDB controller and leader scopes
were pending; the 14 other scopes passed. See
[the composition record](native-result-composition.md) and
[checkpoint inventory](native-checkpoint-inventory.md).

The final native captures used the existing `arroyo_sql_testing-df802a90a8396c4e`
executable from the 606-library-test/full-workspace-build candidate; only
fixtures and documentation changed after that build. Native UUID dependent-write,
fan-out and recovery passed all eight memory/RocksDB × batch target 1/8 ×
controller/leader captures at `target/native-uuid-final-combined`. Closed-HOP
reaggregation passed the same eight-case matrix and strict value/recovery oracle
at `target/native-window-composition-final-combined`. The external native identity
query passed all eight captures; the application-owned adapter/comparator passed
16 strict initial/recovered comparisons, each with 13 unified events and two
directed merges, recorded in
`target/native-identity-final-combined/comparisons.json`. Application SQL,
oracle and comparator remain external to Streamr. These fixture results do not
establish full M3 completion, profile/session parity, packaged fault handling or
capacity qualification.

## Execution accounting

```toml
[worker.execution-resources]
memory-bytes = 16777216
max-batch-bytes = 1048576
```

This opt-in test configuration shares one DataFusion memory pool between live
stateless physical executors and projection operators. Plan decoding and execution
use the same runtime. Inputs and outputs have explicit batch limits, and reservations
remain active across collector backpressure. Output streams poll lazily without
prefetch queues. Cancellation, completion and errors release reservations and any
unused input slot. A stateless executor rejects a second input until its previous
stream completes or is dropped. Invalid configuration and budget changes while a
bounded runtime remains live fail explicitly.

The pool accounts cooperative DataFusion execution and these caller boundaries;
it is not a process RSS cap. Kernels allocate output before its batch size can be
checked, and arbitrary UDF allocations are not governed by this pool. Retaining
or cloning a yielded batch after requesting the next one requires caller
accounting. `process_single` returns a caller-owned result and propagates errors.
Generic window/join input channels and histories are not migrated by this slice.
Graph queues retain message-count bounds; byte admission for their retained batches
remains part of STR-16.
Spilling is disabled until disk capacity and cleanup are qualified; resource
exhaustion returns an error. Existing disk SQL retained-operator rejection remains.

## Durable timers and collections

`LiveTable::timers` constructs `DurableTimers` over a registered namespace.
Event and processing clocks have independent primary/deadline indexes within
that same namespace. Replacement, cancellation and related state changes can
commit in one backend batch. Snapshot scans include the exact deadline boundary,
page through arbitrarily many IDs at one timestamp, and bind cursors to the
snapshot, clock and cutoff. Before firing, the operator revalidates the current
deadline/payload against a scanned entry. A stale page cannot cancel a replacement.

`LiveTable::ranked_counts` constructs separately addressed counters and ranking
entries within one registered namespace. Entity keys use length framing; callers
choose and encode logical identity and incarnation where needed. Updating a counter and its previous/new rank is
atomic. Ranking is descending count, then ascending member bytes. This is the generic primitive's
explicit deterministic ordering contract. Counter overflow/underflow and oversized values fail. Top-K reads,
membership scans and retired-incarnation cleanup are bounded by bytes and entries.

Both views require one serial execution owner. Prepared mutations hold that
ownership through the combined state/index commit; they do not implement CAS.
Callers must admit input/related-state assembly and retain prepared-update/page
reservations for their documented lifetimes. `with_resources` validates buffer
and nested-read headroom and charges retained results to the worker state pool.
No hidden namespaces are used, so the milestone 2 full logical snapshot exporter
captures each primary/index relationship together. Schema identities must version
the operator's logical state. Restore always targets a fresh attempt.

These APIs do not schedule callbacks by themselves. Native window operators
already own watermark-driven scheduling; their state and execution still need
the selected migration/qualification work. Use native aggregate/window SQL
composition first. If actual tests demonstrate a missing timed-output behavior,
discuss the minimum generic interface before implementing it; no external
application callback path is preselected for this milestone. Any such path
would need state registration, clock/watermark/recovery ordering, admitted typed
outputs and source/sink checkpoint integration. Storage tests establish none of
those execution guarantees.

## Reproduction

Use the prescribed Bookworm image and migrated build database from
[milestone 2 setup](milestone-2-validation.md#reproduction). Preserve its warm
Cargo target. The first-slice verification command is:

```sh
bash scripts/verify-milestone3.sh
```

The script needs no application checkout. It runs state/RPC/worker units,
SQL-testing compilation, all-target checks, strict Clippy, formatting and diff
checks in the prescribed development container.

External applications may use the opt-in `external_sql_checkpoint_capture` test
with an externally prepared single-file SQL query. The harness accepts absolute
query/output paths and explicit expectations; it has no business schema or oracle.
Set `STREAMR_CAPTURE_QUERY`, `STREAMR_CAPTURE_OUTPUT`,
`STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT`, `STREAMR_CAPTURE_EXPECTED_ROWS`,
`STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS`, and `STREAMR_CAPTURE_CHECKPOINT_EPOCH`.
Optional `STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS` declares the uninterrupted
capture count separately; it defaults to `STREAMR_CAPTURE_EXPECTED_ROWS`.
Updating aggregates can emit additional intermediate changelog rows when a
checkpoint forces a flush. Compare their complete values and final materialized
result as well as the explicitly declared counts.
Use `STREAMR_TEST_BACKEND=memory|rocksdb`,
`STREAMR_TEST_CHECKPOINT_MODE=controller|leader`, and optional
`STREAMR_TEST_EXECUTION_BYTES` for the engine configuration. Run it alone:

```sh
cargo test --locked -j4 -p arroyo-sql-testing external_sql_checkpoint_capture \
  -- --ignored --test-threads=1 --nocapture
```

The source must be a single control-waiting file source; the graph must have
parallelism one. Expected output counts are independent of input counts, so
filters and multiple state owners are not assumed to preserve rows one-for-one.
The initial capture uses `.initial.jsonl`; the recovered capture uses the supplied
output path. The harness recreates the program from the selected published
checkpoint and source/sink metadata. Worker cancellation is not a process-kill,
Kafka, remote-storage or beyond-RAM proof. Fixture preparation, application output
schemas, reference normalization and business comparisons belong externally.

Earlier compatibility evaluation used a pinned external Arcstream fixture at
`10f779469748d0c0dde6d5af3aa7375f8dbc36d3`. Its six initial/recovered identity
comparisons passed at Streamr `a2d2aba7` with 16 MiB execution accounting. That is
historical external evaluation evidence, not an engine dependency or profile/session
readiness claim. The timer and collection tests separately exercise the generic
production logical exporter and fresh RocksDB restoration.

## Validation status

Qualification of `a2d2aba7` passed 153 Bookworm units (33 RPC, 56 state, 64
worker), 25 isolated native fixture runs, all-target checks and strict Clippy.
CI passed 468 library tests and 12 integration tests. Validation for the current
application-boundary correction is recorded against the exact PR head in
[PR #6](https://github.com/jasadams/streamr/pull/6); no pending check counts as
acceptance.

STR-16, STR-29 and the STR-28 inventory remain in progress. STR-17, STR-20, STR-26
and STR-32 retain their full acceptance gates. Milestone 3 requires reviewed
foundation/map artifacts, profile/session parity, actual hot-key and >=10x state
budget qualification, representative backfill and a 24-hour live/fault run before
readiness. No release, deployment or canonical-topic change is part of this branch.
