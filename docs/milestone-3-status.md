# Milestone 3 — agent backlog

## Active STR-102 — 2026-10-10

- Coordinator Codex `/root`, implementer `/root/implement`; branch
  `jason/str-102-updating-joins-20261010`, fetched base
  `130f43a265551883e8a13c1d55d51697e27e7f9f`; start 11:57 UTC.
- Instructions: `AGENTS.md`, `.claude/team.md`, `.claude/build-test.md`,
  this ledger and `milestone-3-work-plan.md`; current Trakkt STR-102 scope.
  No automatic elapsed-time limit or signing requirement.
- Acceptance: existing ANSI updating aggregate INNER/LEFT joins, exact bag/CDC
  identities and transitions, bounded generic state/probes/output, both backends,
  checkpoint/fresh-worker regressions, affected-crate gates, independent review
  and green open PR. STR-65 retained-table feeds/bootstrap and STR-21 historical
  joins remain separate.
- Native planner/runtime support and configured row/probe/output bounds are
  implemented; seven physical-plan worker regressions and a 32-case SQL matrix
  are prepared. Upstream PR420 supplies the changelog/LEFT transition reference;
  old execution paths are not restored.
- All current gates passed: affected four-crate locked check/strict Clippy/fmt;
  seven physical-plan worker regressions, one serialized loader-admission test;
  planner library 219 passed/one existing ignored; Python oracle 16 passed.
- All 32 actual SQL configurations passed independent complete-value/CDC oracles
  and checkpoint/fresh-worker continuation: direct INNER/LEFT, chained INNER,
  downstream updating aggregate × memory/RocksDB × controller/leader × batches
  1/8. Source/build/configuration receipts and failed attempts are preserved in
  `/home/jason/qa-evidence/str102-20261010/`; see `str-102-validation.md`.
- Existing test overrides provide scan 16 MiB and queued writes 64 MiB for the
  declared graph; product limits/checkpoint contracts are unchanged. Independent
  source reviews approved all material repairs; pre-rebase evidence approval passed.
- Rebased onto main `4e881205` after independent approval; original acceptance
  evidence remains preserved. Next: combined-source gates/matrix, integration
  review, publish and current-head CI. Heavy capacity/RSS,
  slow-consumer/fault/soak qualification remains the existing STR-32 shared batch;
  this feature increment does not establish milestone qualification. No user
  decision pending.

## Active STR-101 — exact-timestamp backend parity

- Codex run `codex-str101-20261010`; branch `jason/str-101-instant-window-20261010`, workspace `/home/jason/repos/streamr-worktrees/str-101-instant-window-20261010`; fetched base `130f43a2`. Instructions: `AGENTS.md`, `.claude/build-test.md`, this ledger, `milestone-3-work-plan.md`, current Trakkt ticket and canonical backlog-fast skill. No elapsed-time limit or signing requirement.
- Acceptance: bounded shared exact-timestamp grouping preserving strict watermark closure, lateness, output values/types and existing checkpoint/recovery; generic nested SESSION fixtures, boundary/recovery/resource/cancellation regressions; independent review, affected-crate gates and green open PR.
- Prior diagnosis: `/home/jason/qa-evidence/str32-20261010-52391450/arcstream-integration/profile-session/session-instant-rocks-gap-review.md`. Upstream supports width-zero grouping; the current bounded adapter excludes it. Existing memory functional passes do not prove bounded resources.
- Implementation source review passed. Window tests 16/16, shared admission filters 3/3, affected worker/SQL-testing locked checks, strict all-target Clippy and formatting passed. Final normal SQL matrix 8/8 (five rows, committed prefix3) and late matrix 4/4 (six rows, committed prefix4) passed exact scalar types/values and fresh-worker continuation. Test-harness cleanup-permit wait resolves native close overlap without extra slots.
- Evidence: `/home/jason/qa-evidence/str101-20261010/`; failed attempts retained. Pinned executable SHA256 `f9b0028b9e7eebccfb794c58aa3d96581351306b48b92b45b169e221314afb93`, source/config receipts retained; queue reservation `318d3019bbbf` exited0. Published PR #33; prior source/evidence review approved. Conflict repair integrates fetched main `025e161360488b1bb50d4cab9457edcebe85cccf` with PR head `06929bd8913db7829b4f1ac5423bd1046a67ed8d` without rewriting history. Only this ledger conflicted; all six executable file blobs are unchanged from the reviewed PR head. Static conflict/whitespace and tree audits passed; no Cargo/build run for documentation-only resolution. Next: fresh repair review, publish the merge commit and verify repaired-head CI. No SQL rewrite or product budget change; nested fixture declares its three-owner scan/write profile. Beyond-RAM/RSS/resource qualification remains STR-32 batch evidence, not established by unit tests or the finite matrix. No pending user decision.

## Active STR-32 Arcstream integration and Kafka idle recovery

- Checksum admission repair merged as PR #31 (`130f43a2`); 92 live-state tests and four finite combined production recovery cases passed independent review.
- User waived standalone 24-hour soak and selected Arcstream integration as the live test. Native identity must use state tables. Application worktree `/home/jason/repos/arcstream-streamr-integration` uses fetched Arcstream base `c7f4777`; original dirty checkout and existing reference branches preserved.
- Current native identity eight-case matrix and initial Kafka 13-event/two-merge oracle passed. An idle checkpoint after offset restore then another resume replayed committed records: 14 raw inputs,27 captures,40 unified outputs,four merges. All three isolated pipelines checkpoint-stopped; live producer not started.
- Root owns generic Kafka repair from fetched `130f43a2`, branch `agent/str-32-kafka-idle-offset-20261010`, worktree `/home/jason/repos/streamr-str32-kafka-offset`. Kafka source matches upstream `74c967e3`; restored startup offsets are not seeded into the map written by idle checkpoints. No new recovery semantics or checkpoint format proposed.
- Repair implemented and source-reviewed. Three real-broker source tests, locked connector check, strict Clippy, format and product build passed. Fresh isolated Arcstream double-restore/idle checkpoint/new input passed exact 14 raw/capture/unified and two merges, stable literal identities and source-offset proofs. Next: independent evidence review and publish this increment; native session diagnostics run serially next. No queue, automatic time limit or pending semantic decision for this repair.
- Evidence `/home/jason/qa-evidence/str32-20261010-52391450/arcstream-integration/` and `kafka-idle-offset-repair/`. Event-clock40 stale-oracle failures after approved watermark admission remain documented; broader capacity/fault/backfill and full profile/session consumer acceptance remain open.

## Active STR-56 continuation — 2026-10-10

- Codex `/root`, start 00:21 UTC; branch
  `jason/str-56-sql-json-delivery-20261010`, workspace
  `/home/jason/repos/streamr-worktrees/str-56-delivery-20261010`.
  Resumed with explicit user authorization on fetched base `48ceb3fe`;
  unpublished Phase B `31928d94` rescued as `db54dad8` with the staged
  continuation patch. Both earlier worktrees remain preserved.
- Instructions: `AGENTS.md`, `.claude/team.md`, `.claude/build-test.md`,
  this ledger, and current Trakkt STR-56 description/comments. Current user
  instructions retire signing and all automatic elapsed-time limits.
- Acceptance remaining: complete SQL projection and consecutive MERGE presence
  gating; memory/RocksDB, source batches 1/8, checkpoint/fresh-worker VARCHAR
  recovery; oversized-operation atomic failure and reservation release; fresh
  independent review, affected-crate gates and green open PR.
- Prior queued validation session `15738` was canceled with exit 130 at the
  user's request; no final validation gate ran. Repairs and runtime fixtures
  remain staged. Formatting, whitespace and harness syntax passed on that
  source; runtime, crate checks and Clippy remain pending. Source review found
  the original defects repaired; final approval awaits validation.
- Current validation: round 4 exact SELECT planner/worker fixture and all eight
  Engine MERGE recovery configurations passed after StringView/MERGE guard
  repairs. Planner 57 tests, worker six tests, locked check and formatting passed.
  Independent material source review approved. Strict Clippy found only decode
  visibility and test Arc-initialization lints; narrow repairs passed final worker
  tests, locked check, strict Clippy and formatting (all exit 0).
  Round 3 stale generated RPC artifacts were recovered with targeted crate
  cleanup; source protocol was unchanged.
- Latest main `8cbd443d` integrated by clean rebase; its unrelated preservation
  tooling/build guidance was reread. Final independent verdict approved publication.
- Published [PR #29](https://github.com/jasadams/streamr/pull/29); ticket In Review.
  Next action: verify current-head CI, then hand the open PR to merge-sweeper.
  Prior evidence is preserved
  in `/home/jason/qa-evidence/str56-20261010/`; resumed receipts go to
  `/home/jason/qa-evidence/str56-delivery-20261010/` and
  `docs/str-56-validation.md`. STR-70 tracks non-blocking parser naming cleanup;
  STR-73 tracks shared-target generated RPC artifact invalidation.
  No pending user decision.

Updated 2026-10-09 at the user's request. Read the current Trakkt ticket description
before claiming work. Historical comments and validation logs are evidence, not
current dispatch instructions. No implementation worker is claimed by this reset.

## STR-71 RocksDB audit and worker WAL optimization — 2026-10-10

- Worker `/root`, branch `audit/str-71-rocksdb`, start 01:05 UTC; base fetched
  `67c242f5f3fbfbe977d8595c2960f83fd9c9bcfc`. Instructions: `AGENTS.md`,
  `.claude/build-test.md`, this ledger; current user overrides historical
  deadlines/signing. Ticket guard clear; current STR-25/26 scopes read.
- Acceptance: independent recovery/performance audits; worker-only WAL disable;
  unlogged memtable update/delete snapshot regression; full logical export/restore
  coverage; container check/Clippy/tests and written review. Diagnostics retain
  synchronous WAL. Acceptance checks below passed.
- Recovery audit: worker construction uses fresh UUID attempts; table-manager
  barrier capture precedes asynchronous full export and completion metadata;
  controller/leader publication chooses recovery; Kafka source checkpoint offsets
  select replay. Exactly-once output remains sink dependent.
- Flush evidence: rust-rocksdb v0.24.0 `src/checkpoint.rs` passes zero flush
  threshold; RocksDB v10.4.2 `db/db_filesnapshot.cc` flushes memtables before file
  capture. This permits WAL-disabled worker puts/deletes without format changes.
- Performance findings at base: `live/rocks.rs` writes perform one old-value
  lookup per distinct key for exact logical telemetry, under a DB mutex; native
  health properties/free-space sampling run every batch. `multi_get` loops
  individual pinned gets; bulk scans populate shared cache; native background
  compaction uses default settings. These require measured tuning, not invented
  production defaults. Shared cache/WBM, admission and pinned value limits remain.
- Largest known cost: ordinary read views create physical checkpoints/read-only
  DBs, including memtable flushes. Existing
  [read-view proposal](rocksdb-read-view-proposal.md) records source-bound evidence
  and lifecycle tradeoffs. Full remote checkpoints
  still scan/upload every row; incremental SST work remains STR-24/25.
- Process-loss gap: `NativeDb::Drop` cannot clean attempts after abrupt death;
  fresh UUID construction has no stale-attempt scavenger. Reclamation needs
  ownership/active-process proof. No shared storage deletion was performed.
- User approved lightweight ordinary snapshots after the lifecycle explanation:
  readers retain the live DB, close/removal waits for their release, durable
  checkpoint capture stays separate. Implement as the next reviewed increment
  after delivery of this WAL change; long readers retain historical versions.
- Additional binding gaps: WAL-disabled shutdown flushes memtables by default;
  Rust 0.24 has no safe `avoid_flush_during_shutdown` setter. Batched pinned
  MultiGet exists but needs explicit CF opening and bounded concurrent value
  pinning; do not replace the safe serial loop without resolving those bounds.
- Resumed with user approval after the build-process pause. Rebased onto
  `48ceb3fee902d80d012be3de3320b018e3418364`; re-read `AGENTS.md` and
  `.claude/build-test.md`. The updated wrapper uses the prebuilt RocksDB image
  and shared target. Independent final review approves with no findings.
- Validation on that base with image `a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`:
  container `check --locked -p arroyo-state` passed (1m13s); strict locked
  `clippy --no-deps --all-features --all-targets -p arroyo-state -- -D warnings`
  passed (17.99s); `test --locked -p arroyo-state --lib live:: -- --test-threads=1`
  passed (91 tests, 4.84s; compilation 1m26s); `fmt --all -- --check` passed.
  Includes unlogged update/delete capture and typed full/empty cross-backend restore.
  Source patch/hash, image receipt and exact logs:
  `/home/jason/qa-evidence/str71-rocksdb-20261010/`. Next: publish this increment.
  Heavy compaction/RSS/fault/soak qualification remains the explicit STR-32 batch.

## Available features

## Active STR-62

- Worker: Codex `/root`; branch `jason/str-62-calendar-2147`, base `67c242f5`.
  Started 2026-10-09 21:47 UTC; no automatic time limit.
  [PR #23](https://github.com/jasadams/streamr/pull/23) delivers the reviewed feature;
  current-head CI and merge handoff remain publication gates.
- Decision: user instructed acceptance revision and implementation of common
  watermark finality. Existing AS is the sole control: drop late source changes
  atomically, preserve admitted CDC trigger context, prune recent buckets from
  finite real progress, retain lifetime/correction metadata. No calendar-only rule.
  Optional additional lateness/corrections is separate STR-69.
- Implementation: source-only envelope marker consumed at common watermark
  admission; signed optional emitted boundary checkpoints in existing state.
  Calendar W/V progress and cursor prune one bounded page at a time, with
  per-family horizon floors; equality/future buckets and G/J survive.
- Publication: reviewed source is pushed in ready PR #23. Next: current-head
  active CI gate, then `/merge-sweeper` owns merge and the Done transition.
  Executable qualification used optimized main `48ceb3fe`; integration onto
  `8cbd443d` preserves every recorded engine/harness hash (`post-rebase-audit.json`).
  No build remains in flight.
- Evidence: locked affected check, formatting and strict Clippy passed; planner
  162/162, calendar worker 12/12 and admission 7/7 passed. Actual SQL 40/40 passed:
  pruning 8, default calendar 16 and expression shapes 16, with 40 fresh-worker
  recovery receipts and 136 live observations. Receipts and pinned binary:
  `/home/jason/qa-evidence/streamr-str62-2147/optimized-pruning-retry3/`.
  `final-matrix-audit.json` and exact test-only patch audit link current passes.
  Independent source/evidence review approved; failed/cancelled attempts preserved.
- Composition: STR-29 retains quiet-key traversal/output ownership; shared full
  capacity/fault qualification remains STR-32, external parity ARC-15/16.
  Separate window CDC deficiency remains STR-68. No remaining retention decision.

At the 2026-10-09 reset, all eight tickets were Todo, agent-ready and unblocked.
STR-16 now has [PR #15](https://github.com/jasadams/streamr/pull/15) open awaiting
final CI and merge. Independent agents may work in parallel in separate worktrees
with the file ownership in each ticket.

| Ticket | Deliverable | Primary ownership |
| --- | --- | --- |
| [STR-16](https://trakkt.app/issues/STR-16) | Bounded queue/transfer ownership through slow consumers and cancellation | Shared resources, graph/network queues, checkpoint permits |
| [STR-17](https://trakkt.app/issues/STR-17) | Bounded native SQL top-K, exact CDC and recovery | Aggregate kernels/planner/ranked collections and aggregate capture assertions |
| [STR-19](https://trakkt.app/issues/STR-19) | Bounded TUMBLE/HOP closure and collection output | Fixed-window stores/operators/fixtures |
| [STR-20](https://trakkt.app/issues/STR-20) | Bounded many-key/hot-key SESSION closure and recovery | Session stores/operators/fixtures |
| [STR-26](https://trakkt.app/issues/STR-26) | Native state health metrics and usable operating limits | Collectors/metric hooks and operations/support docs |
| [STR-42](https://trakkt.app/issues/STR-42) | Three-epoch nonempty/changed/empty state-table recovery | Conformance driver/fixtures and multi-epoch capture scenario |
| [STR-29](https://trakkt.app/issues/STR-29) | Watermark-driven zero counts for quiet retained keys | Result composition/expiry, coordinated with STR-19 |

| [STR-43](https://trakkt.app/issues/STR-43) | Complete removal of legacy state SQL and processor code | Legacy catalog/planner/runtime/protocol paths and their fixtures/docs |

One feature per worker. Claim with worker/session, start, acceptance and owned
files; only then set In Progress. Coordinate shared sections before editing.
STR-17 owns aggregate capture assertions in smoke_tests.rs; STR-42 owns the
multi-epoch scenario. Compiler/capacity jobs remain serialized through cargo-dev.

## Active STR-16 increment

- Worker `/root` on `jason/str-16-ownership-cancellation`, start 2026-10-09
  00:39 UTC; deadline 01:39 UTC. Owned queue/network and checkpoint admission paths.
- Repair: RocksDB decoded scan pages retain their existing scan reservation until
  page consumption/drop. Regressions cover retained pages, data/signal queue
  cancellation/drop, checkpoint export/restore cancellation and fresh retry.
- Independent review approved. Affected-crate compile and Clippy checks passed
  (exit 0) using documented immutable dev image `cdeb96c9e3e`. At `6bb22ee7`,
  `cargo-dev test --locked -p arroyo-state -p arroyo-operator -p arroyo-worker cancel`
  passed (exit 0): 3 operator, 10 state and 15 worker tests. Source-bound log:
  `/tmp/str16-cancellation-tests-final.log`.
- [PR #15](https://github.com/jasadams/streamr/pull/15) is open awaiting final CI
  and merge. Heavy slow-consumer/RSS/storage-fault/soak qualification stays with
  STR-32. No user decision or read-view redesign is required. Next action: finish
  final PR CI and merge review.

## STR-26 integration repair

2026-10-09 worker `health_repair`, start 21:26 UTC, deadline 22:26 UTC.
PR #16 is being merged with current main `d0d910e3`; preserve native health
telemetry and current engine ownership/clock contracts. Only documentation
conflicts required manual resolution; source merged automatically. Local builds,
tests and Clippy not run: documentation-only repair. Staged whitespace and
unresolved-conflict checks passed. Acceptance/next action: independent review,
publish the repair and verify current-head CI. No user decision is pending;
actual load/restart/sizing qualification remains STR-32.

Prior implementation evidence (not current integration validation):

2026-10-09 worker `str-26-native-health`, started 01:22 UTC, deadline 02:22 UTC:
implementation complete and staged: logical accounting, weak cached SST/free/
stall observations, execution refusal/reservation classes, monotonic publication/
initialization/local readiness timings, and operating/support docs. Current-source
repaired-Bookworm/shared-target gates passed: narrowed state/worker check,
state health tests 2/2, worker execution/lifecycle metrics tests 7/7, required
all-feature/all-target preflight Clippy, and workspace formatting. Independent
source review found no issues; exact next action is final reviewed receipt and PR
delivery. Evidence: `/home/jason/qa-evidence/streamr-str26-20261009/` (base SHA,
staged patch/hash, image ID, commands, exit statuses and logs). Actual load/restart
scrapes and production sizing remain STR-32; total allocated directory bytes,
cumulative stall duration, per-resource reservation attribution and end-to-end
outage time are explicitly unsupported. No SQL, admission or checkpoint semantics
change; no user decision or local verification blocker remains.

## Decisions and final acceptance

STR-17 continuation: Codex `/root` and `/root/implement` started 2026-10-09
10:31 UTC. The user removed the one-hour limit; continue through green PR CI.
Workspace:
`/home/jason/repos/streamr-wt-str-17-bounded-top-k`, branch
`jason/str-17-bounded-top-k`, base `b3930c00`. The previous worktree was clean;
no bounded top-K implementation or PR existed. Acceptance: bounded finite-K
selection preserving SQL/CDC/recovery, exact capture assertions, focused
regressions, affected-crate gates and independent review. Planner/runtime bounded
selection and capture regressions are committed and independently approved.
Seven Python capture self-tests passed. Source `29966185` passed affected-crate
check, strict all-target Clippy, three planner tests, four worker tests and
formatting in the documented repaired image `cdeb96c9e3e9`. The worker tests
include actual checkpoint export/restore to memory and RocksDB. Earlier image,
compiler and lint failures were repaired; final batch exit was 0.
[PR #21](https://github.com/jasadams/streamr/pull/21) is open; `/merge-sweeper`
owns the merge after current-head CI passes.
No pending product decision; full runtime/capacity qualification stays in STR-32.

- STR-29's contract was approved on 2026-10-09: watermark expiry emits zero
  rolling counts for retained lifetime/key rows; complete input silence does
  not advance event time. Ordinary window behavior remains unchanged.
- STR-43 is approved and ready: remove the legacy implementation completely.
  Streamr is pre-release; no compatibility shims or migration tooling are required.
- STR-32 holds one shared operator/combined capacity, resource, backfill,
  process-loss/storage-fault and actual 24-hour acceptance checklist. Heavy runs
  require an explicitly requested shared batch; held/cancelled cases stay held.
- STR-44 optional per-group emission remains post-MVP. Read-view lifecycle
  redesign remains unapproved. Application schemas/parity stay external.

STR-28 is folded into STR-26. STR-49/50 into STR-17; STR-51 into STR-29;
STR-52–55 into STR-32. These are Cancelled duplicates, not another queue.
STR-38–41 implementation is Done on its recorded own-scope acceptance;
STR-45/47/48 repairs are also Done. Earlier STR-48-only dispatch is superseded.

## Evidence

Reuse source-specific results without presenting them as fresh qualification.
See milestone-3-validation.md, batch-qa-2026-10-08.md and
state-table-all-key-capacity.md. Basic native operators, merged state tables,
strong window/hot-session and selected all-key capacity results already exist.
Later cancelled reruns do not erase earlier passes or authorize another run.
Full current-candidate qualification remains STR-32; no test was run by this
backlog rewrite. Git history preserves the previous working ledger.

## STR-20 implementation ready — 2026-10-09

Worker `jason/str-20-session-bounds`, started 08:22 UTC; deadline 09:22 UTC.
The session operators/store and fixture increment passed the focused verification
below. Current independent review, PR and CI outcomes are recorded on STR-20.

Acknowledged closure is persisted before paged retirement. Cancellation before
acknowledgment retains complete history; cancellation afterward resumes deletion
without re-emission, including full checkpoint restore into a fresh backend.
One bounded in-worker acknowledgment is flushed before checkpoint capture.
IPC rows are compacted within the existing reader admission to avoid shared-body
memory overcounting. Native ARRAY_AGG accepts direct source value/order/filter
columns, uses paged input/value admission and separately reserved execution
scratch. Its planner Final/Partial pair is locally normalized with original order,
filters and unchanged output schema. Oversized values error without truncation or
budget increases. Session format/identity is version 2; disk encoding stays 1.

Executed in verified Bookworm image `cdeb96c9` on final source:
- `scripts/cargo-dev check --locked -p arroyo-worker` — exit 0.
- `scripts/preflight-clippy.sh -p arroyo-worker` — exit 0.
- `scripts/cargo-dev fmt --all -- --check` — exit 0.
- `scripts/cargo-dev test --locked -p arroyo-worker --lib arrow::session -- --test-threads=1`
  — exit 0; all 21 tests pass. Coverage includes actual planner/constructor typed
  integer/text/Boolean arrays and filtered first/last metadata, expanded-expression
  rejection, slow collection, cancellation before/after acknowledgment, full
  checkpoint/fresh-backend partial retirement, remaining open state, reused keys,
  24-key one-entry-page closure and oversized hot-collection history preservation.

Earlier Clippy exited 1 on new test helper visibility/storage-access errors; fixed.
Earlier focused tests exited 101 (18/21 then 20/21 passes): IPC backing-memory
admission, fresh database ownership, and physical-schema alias assertions were
repaired without increasing limits. These failures do not establish passing results.
Prepared SQL backend/protocol/reused-key fixtures are not runtime matrix passes.
Historical 12 normal/late and hot capacity results stay source-specific. Full
fixture matrix and heavy capacity/RSS/fault/soak qualification remain STR-32.
Read-view lifetime and stopped-input event-time semantics are unchanged.

## STR-61 reviewed integration repair — 2026-10-09

- Published branch `jason/str-61-event-clock-092033`, repaired source `e4ad163b`,
  integrates main `811433d1` without rewriting history. Start 21:00 UTC; deadline 22:00.
- Acceptance: resolved conflicts, independent review, affected-crate locked check,
  strict Clippy, focused event-clock regressions and repaired-head CI.
- Conflict: planner filter rewrite retains row event-clock binding and removes
  obsolete scalar-state function handling as required by current main.
- Current-source eight-crate locked check and strict Clippy passed (exit 0);
  all 18 focused regressions passed. Logs: `/tmp/str61-integration-check.log`,
  `/tmp/str61-integration-clippy.log` and `/tmp/str61-integration-*-tests.log`.
- Fresh SQL worker recovery passed all 48 cases on `e4ad163b` (exit 0), with
  memory/RocksDB, batches 1/8 and controller/leader checkpoints. Preserved executable
  SHA256: `ac282bfa24b5f59ccc8fdcd2d26d33766145037efbb580f25496276151572395`.
  Source receipt, build/runtime logs and captures:
  `/home/jason/qa-evidence/str61-integration-20261009T2100Z/`.
- Independent integration review found no new issues; all six CI checks passed
  on `e4ad163b`. PR #22 is mergeable. Next action: verify CI after this evidence-only
  ledger commit, then hand the open PR to merge-sweeper. Full server/browser QA
  remains batch verification; STR-32 owns combined capacity/fault qualification.
- STR-62 maintained aggregates and STR-67 signed window cutoffs stay deferred.
  No new semantics or user decision is introduced by this integration.

## Active STR-17 integration repair — 2026-10-09

- Existing PR #21 branch `jason/str-17-bounded-top-k`, head `38cec514`, integrates
  main `d0d910e3` without rewriting history. Start 21:26 UTC; deadline 22:26 UTC.
- Acceptance: resolve conflicts, preserve bounded finite-K selection and all
  native main changes, static checks, independent review and repaired-head CI. No new semantics or product decisions.
- Only conflict: AGENTS.md retains current main instructions; the old branch's
  deadline removal is discarded. Planner clock/top-K and protocol changes merged
  automatically. Local builds/tests/Clippy not run: documentation-only repair.
  Staged whitespace and unresolved-conflict checks passed; previous feature results
  remain source-bound historical evidence.
- Next action: independent review, publish the repair and verify current-head CI.
  Full SQL/capacity/slow-consumer/process-loss qualification remains STR-32.
