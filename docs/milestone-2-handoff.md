# Milestone 2 agent handoff

Implement recoverable disk-backed SQL maps in Streamr, with a usable test candidate
as early as possible. Own STR-10 through STR-15 and coordinate their SQL prerequisites
STR-1 through STR-4. Bring runtime testing forward, keep integration behind an
explicit opt-in setting, and finish with evidence from actual SQL execution,
checkpoint publication and recreated-worker recovery.

Project: https://trakkt.app/projects/c286663a-6ec6-4f5a-a129-d5d1f2d8e17f

## Starting revision and existing work

Repository: `jasadams/streamr`. The default branch is `master`.
Foundation implementation: commit `e7de470d67c707ccec0d9528e1d1d4202adc8949`,
branch `feat/str-rocksdb-foundation`, [PR 2](https://github.com/jasadams/streamr/pull/2).
At handoff preparation on 2026-10-03, PR 2 is open and draft, not merged. Verify its
current state before branching. Use updated `master` once it contains the foundation;
otherwise create an isolated worktree based on the foundation branch and make the
PR stacking relationship explicit. Do not reimplement the foundation.

Read [live state foundation](live-state-foundation.md), the project design in
Trakkt, `.claude/build-test.md`, and the current ticket descriptions before editing.
`docs/disk-backed-state-design.md` is an existing untracked user file in the
foundation worktree; do not discard or silently include it in an unrelated commit.
The design is also available in the Trakkt project, so a fresh checkout does not
require that local file.

The primary checkout `/home/jason/repos/streamr` is on
`jason/str-1-stateful-planner` and has uncommitted planner, worker, SQL-testing and
candidate-build work. Inspect its current diff and coordinate ownership before
integrating any of it. Preserve edits and untracked files; do not reset, overwrite
or automatically cherry-pick an assumed implementation. Its `.claude/team.md`
identifies team STR and the Streamr repository. Internal Arroyo crate names remain.

## Foundation capabilities and limits

`crates/arroyo-state/src/live` provides owned async reads, atomic bounded writes,
stable local snapshots, exact-range cursors, ownership/versioned encoding, worker
resource admission and paginated Arrow histories. `LiveTableManager` is available
alongside the legacy table manager. RocksDB is pinned to Rust binding 0.24.0 and
native engine 10.4.2 with LZ4 and runtime bindgen.

Use `RocksLiveState::open_worker` and the process-wide resource pool in worker
execution. Use `admitted_batch` before assembling pending native writes;
`write_admitted` retains admission through completion. Ordinary owned write inputs,
SQL overlays, input/output buffers and extracted Arrow batches still require
producer accounting. Budget cache, memtables, scans, decoding and storage requests
across operators; cache admission is not a total RSS cap.

The worker resource configuration is explicit and has no production defaults.
It does not select a backend. Milestone 2 must add backend selection and wire it
through actual job/operator construction. Local `reopen` is a diagnostic operation,
not pipeline recovery. Recovery must restore a selected committed checkpoint into
a fresh attempt directory, rather than trust local WAL contents.

Foundation validation in Bookworm Linux x86_64 passed 29 state and 32 RPC tests,
all-targets checks, changed-crate Clippy and formatting. The four-database native
probe stored 256 MiB with 11 MiB assigned budgets, reopened and checked every
payload; reported peak RSS was 34.1 MiB. This is backend evidence only. Rust CI was
queued, distribution targets were not qualified, and workspace formatting had
pre-existing differences in planner and worker stateful-SQL files. Recheck current
CI rather than treating those observations as current results.

## Delivery order and early candidates

| Stage | Work | Required result |
| --- | --- | --- |
| Runtime baseline | Harness portion of STR-2 and STR-15; coordinate STR-1, STR-3 and STR-4 | Planned SQL executes through the actual worker; outputs and state effects are asserted |
| SQL preview | STR-10 and opt-in construction from STR-15 | Bounded RocksDB map access with correct row/expression order; explicitly a development preview until recovery works |
| Recoverable alpha | STR-11, one supported path in STR-12, STR-13 and safe cleanup from STR-14 | Durable barrier snapshot, publication, worker loss and restore pass in the documented checkpoint mode |
| Complete milestone | Remaining STR-12/14 protocol support and STR-15 qualification | Both controller and leader modes pass larger-than-RAM execution, export, restore and retained-checkpoint cleanup |

An early alpha may support one checkpoint mode, provided the other is rejected
explicitly and documented. This intermediate candidate does not complete STR-12,
STR-14, STR-15 or milestone 2. Select the first mode based on the evaluation harness
and record the choice before changing protocol code. Do not silently change the
existing acceptance requirement to support both modes.

Start with supported singleton execution and unchanged parallelism. General routed
maps, rescaling, aggregations, windows, joins and incremental/shared SST checkpoints
belong to later milestones. Use full logical snapshots whose remote files are
owned exclusively by one checkpoint. Unsupported disk-backed configurations must
fail clearly rather than imply all retained operator state is bounded.

## Workstream ownership

The coordinating agent owns the end-to-end harness, feature configuration,
integration and final checks. Suitable independent work can be delegated after
agreeing on the checkpoint metadata, ownership and execution boundary.

| Workstream | Tickets and starting files | Coordination boundary |
| --- | --- | --- |
| SQL semantics and live execution | STR-1 through STR-4, STR-10; `arroyo-planner/src/extension/stateful_processor.rs`, `rewriters.rs`, planner tests; `arroyo-worker/src/arrow/stateful_processor.rs` and constructor dispatch in `arrow/mod.rs` | One owner edits the processor/planner boundary; coordinate with existing STR-1 work |
| Snapshot export and restore | STR-11, STR-13; `arroyo-state/src/tables/table_manager.rs`, `tables/mod.rs`, `global_keyed_map.rs`, `parquet.rs`, live snapshot API; `arroyo-operator/src/context.rs` and worker `engine.rs` startup | Agree on capture timing, logical file format and construction/restore interfaces before parallel edits |
| Publication and cleanup | STR-12, STR-14; `arroyo-rpc/proto/rpc.proto`, `arroyo-worker/src/job_controller`, `arroyo-controller`, `arroyo-state-protocol/src/{types,resolve,workflow,gc}.rs`, state metadata/cleanup dispatch | One owner changes protocol schema and shared dispatch; preserve connector commit data |
| Qualification and candidate | STR-2, STR-15; `arroyo-sql-testing/src/smoke_tests.rs`, query fixtures/golden outputs, `integ/tests/api_tests.rs`, worker integration harness | Tests use public runtime boundaries; coordinator owns evidence and candidate build |

Namespace/proto changes and table-manager edits are shared dependencies. Assign
one owner per file until interfaces are settled; merge small reviewed changes
frequently. The end-to-end coordinator remains responsible for both protocols and
must wait for required delegated work before reporting completion.

## SQL correctness requirements

STR-3 records a confirmed defect: same-named maps in separate CTE stateful operators
have separate maps and checkpoint owners. Establish one ordered, checkpoint-consistent
execution boundary for those named maps; a process-global uncheckpointed database
is not a solution.

STR-4 records a confirmed defect: extracted state operations can run from unselected
CASE branches. Preserve guarded evaluation and audit related lazy expressions;
reject unsupported forms precisely until they are supported. Assert state effects
as well as output values.

STR-1 has existing edits concerning CTE column collisions and logical/physical
schema alignment. Reproduce their runtime effect before choosing which changes to
integrate. Existing worker tests mostly repeat HashMap operations; they are not an
oracle for actual `process_batch`, `handle_checkpoint` or `on_start` correctness.

For the RocksDB path, remove startup `get_all`/complete map clones and unbounded
dirty-key tracking. Process bounded chunks with a byte-accounted overlay so repeated
keys and dependent operations observe earlier writes. Commit each chunk before
emitting its output. A prefetch followed by independent writes must not reorder SQL
semantics. Use a corrected memory implementation and explicit expected outcomes
for parity; agreement with an existing defect does not establish correctness.

## Checkpoint and recovery requirements

Capture all logical tables at the aligned operator barrier after pre-barrier writes
finish and before post-barrier mutations. Export the stable snapshot with bounded
pages, buffers, uploads and overlapping snapshots. Report completion only after
files and metadata are durable. Preserve source offsets, sink commit/replay,
checkpoint-stop and generation fencing through the existing publication protocols.

Introduce versioned disk-state metadata instead of redefining existing Parquet table
metadata. Record schemas, ownership, encoding, checksums, complete file lists and
explicit empty state. Restore only the protocol-selected checkpoint into a fresh
database through bounded streaming writes. Define legacy import support or precise
incompatibility errors. Failed/partial restore must retry without accepting leftover
state as committed state.

Confirm the suspected dirty-only checkpoint defect with two real checkpoints and
an unchanged key. Fix any demonstrated baseline lifecycle defect in STR-2, and use
full snapshot export on the disk path so unchanged keys are included.

Cleanup must preserve retained checkpoints and active publication. Exclusive-file
ownership simplifies this first release, but abandoned uploads still need safe,
idempotent cleanup and interruption tests. Shared-file reuse and incremental native
snapshot retention remain later work.

## Acceptance and evidence

Extend `crates/arroyo-sql-testing/src/smoke_tests.rs` first. It already plans SQL,
runs the real worker `Engine`, checkpoints epochs 1/2/3, compacts and restarts.
Use its query fixtures and golden outputs instead of creating another storage-only
harness. Its current checkpoint path directly uses legacy `CheckpointState`; its
graceful restart does not establish crash rollback or leader-manifest publication.
Add separate crash/recreated-worker and leader-mode coverage. Run process-memory
qualification in a dedicated process with one consistent worker resource budget,
since configuration and the worker resource pool are process-global.

Extend this coverage through the following cases:

- All five state functions, NULLs, repeated keys within/across batches, ordered
  multi-operations, chained CTE maps and unselected conditional branches.
- Two checkpoints with an unchanged key, deletion followed by recovery, and explicit
  empty state. Writes after capture must not leak into the captured checkpoint.
- Worker recreation with newer uncommitted local writes; recovery uses the selected
  committed epoch and follows the existing source/sink replay contract.
- Failure during upload and before/after publication, stale generations, disk/storage
  errors, missing/corrupt files, incompatible schemas and interrupted restore.
- Retained checkpoints remain restorable after cleanup, aborts and cleanup retries.
- Actual SQL execution, checkpoint export and recreated-worker restore with logical
  state at least 10 times the assigned state-memory budget, under a declared total
  RSS envelope. Measure memory after warm-up as cardinality grows at fixed budgets.

Record source revision, configuration, hardware, input/cardinality/payload shape,
uniform/skewed workload, throughput, lag, tail latency, RSS, local disk use and
checkpoint/export/restore duration. Quantify delivery duplicates where applicable;
do not infer exactly-once guarantees from state durability alone.

Use the prescribed Bookworm development container for compilation/tests. Run
changed-crate tests, all-targets check, Clippy with warnings denied, formatting and
`git diff --check`; run the real SQL and recovery harness. Include worker/planner,
state/protocol, RPC and controller checks as the edits require. Do not introduce
vendor/CFLAGS workarounds. Preserve warm native caches to shorten iteration.

Each reviewable candidate should have a reproducible build recipe, pinned revision,
image digest when built, one verification command, evidence and documented supported
modes/limits. Reconcile workspace formatting and runner availability so the Rust
check can actually execute; do not label queued CI as passing.

## Completion and scope

Keep STR-10 through STR-15 in their actual implementation/review state. Do not close
STR-5 after map-only acceptance. Milestones 3 and 4 and Arcstream readiness remain
separate. Inspect tracker dependencies if they impede scheduling; the STR-5/ARC-17
completion relationship appears circular and needs a deliberate tracking correction,
not a claim that either feature has qualified.

Preparation and implementation do not authorize production/demo cutover, canonical
topic changes, license changes or release publication. Build reviewable test
candidates and follow the release workflow only when publishing is requested.

## Suggested prompt for the next agent

> Implement milestone 2 for the Streamr RocksDB project. Read
> `docs/milestone-2-handoff.md`, `docs/live-state-foundation.md`, the Trakkt project
> and STR-10 through STR-15. Start from the merged foundation, or explicitly stack
> on PR 2 if it is still open. Inspect and coordinate the existing STR-1 work without
> overwriting it. Extend the existing real SQL smoke harness first; resolve named-map and
> conditional-semantics prerequisites, then integrate bounded SQL state and committed
> checkpoint recovery. Delegate independent work with explicit file ownership.
> Produce an early documented recoverable alpha, then complete both checkpoint modes
> and the larger-than-RAM acceptance gate. Preserve existing behavior behind an opt-in
> backend, record required checks/evidence and open reviewable PRs. Do not publish a
> release or deploy, and do not close the parent feature at map-only completion.
