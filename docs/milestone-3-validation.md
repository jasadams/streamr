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

## Current STR-28 evidence gate

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
STR-32's backfill/live gates. The new batch's full workspace build and
current-head CI remain pending; the foundation's green CI does not cover it.
The workspace all-target check and strict Clippy passed for this integration
batch. The workspace library run initially failed six Kafka/MQTT connector
tests because local brokers were absent. With dedicated Kafka 3.9.2 and
Mosquitto test brokers running in the build container's network, its unchanged
workspace connector executable passed all 60 tests (container log
`/tmp/streamr-m3-final-connectors.log`). The final timestamp
projection regression also passes the planner's 125-test library suite.

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
