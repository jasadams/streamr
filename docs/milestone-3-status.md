# Milestone 3 — agent backlog

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
- Next action: finish narrow gates/evidence, integrate latest main's unrelated
  worktree preservation tooling, obtain final review and publish the open PR.
  Prior evidence is preserved
  in `/home/jason/qa-evidence/str56-20261010/`; resumed receipts go to
  `/home/jason/qa-evidence/str56-delivery-20261010/` and
  `docs/str-56-validation.md`. STR-70 tracks non-blocking parser naming cleanup;
  STR-73 tracks shared-target generated RPC artifact invalidation.
  No PR or push; ticket In Progress. No pending user decision.

Updated 2026-10-09 at the user's request. Read the current Trakkt ticket description
before claiming work. Historical comments and validation logs are evidence, not
current dispatch instructions. No implementation worker is claimed by this reset.

## Available features

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
