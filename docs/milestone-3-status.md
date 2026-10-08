# Milestone 3 — current working ledger

Read this first when resuming. Historical evidence belongs in
[milestone-3-validation.md](milestone-3-validation.md); manageable tasks are in
[milestone-3-work-plan.md](milestone-3-work-plan.md).

## Current ticket: STR-38 review handoff

User resumed work with two constraints: ONE ticket at a time, and pause/report
if unfinished after one hour. No automatic switch to another ticket.

- Start: 2026-10-08 04:44:16 UTC / 15:44:16 AEDT.
- Deadline: 2026-10-08 05:44:16 UTC / 16:44:16 AEDT.
- Outcome: native state-table DDL/catalog/execution contract, already implemented
  in draft PR #6; audit all four acceptance requirements, close real in-scope
  gaps, independently review and deliver ready for review. No merge/Done.
- Acceptance: parser/catalog/plan diagnostics and composite ownership tests;
  documented visibility/limits/compatibility/retention/parallelism; event-driven
  current-row lookups without retained history; serial per-event read/write/
  output scope and checkpoint replay without per-row database transactions.
- Result: all four requirements have implementation and executed evidence; the
  only correction is the documented worker backend configuration path. See
  [STR-38 acceptance](str-38-acceptance.md). No engine behavior changed.
- Delivery: publish the reviewed documentation increment, verify latest PR #6
  checks and hand STR-38 to review. Tracker/PR record the publication outcome.
  Stop this turn after that handoff; no next ticket has started. No local build
  or runtime is active.

## Frozen source and existing evidence

Implementation HEAD `9bfb7df8aa8d016cfd07e49d59ebb2112611a09e`, draft PR #6. Seven
current-head CI checks green; both Rust workflows passed 715 library tests
(six skipped) and 12 integrations. Local v16 five gates and 12 recovery smokes:
`target/native-m3-reviewed-repairs-v16-validation/` (review-actual-evidence.md).
Reuse existing exact-source evidence before launching new runs.

Recent M3 component evidence, not STR-38 completion or full M3:
- Both large hot SESSION protocols passed 65,000 checkpoint rows / 532,480,000
  decoded payload bytes (10.15625x50MiB): native-session-large-v16-preflight
  and native-session-hot-controller-v16-preflight under target/.
- Four production worker-loss and two controller-loss cases passed exact 6,000
  rows/CDC/checkpoint restoration and 177 attributed metrics samples total.
  Reports: target/native-production-v16-preflight-v2/review-result.md,
  target/native-production-worker-matrix-v16/review-result.md and
  target/native-production-controller-matrix-v16/review-result.md.
  Processes required forced cleanup after Finished; graceful exit unqualified.
- All-key state-table capacity wave session 51984 was canceled during controller
  case following user's concern. Actual owned group stopped/reaped, leader never
  started, queue released. target/native-state-table-all-key-v16-preflight/
  root-cancellation.json; no capacity PASS or engine-failure diagnosis.

## Decisions and milestone blockers

Native RocksDB read snapshots replacing physical read checkpoints remain
UNAPPROVED; user asked to discuss. Stable reads stay, proposed close/removal
would wait for readers; no lifecycle change implemented. Many-key SESSION and
large aggregate capacity remain unqualified. Rolling quiet-key count remains 1
instead of 0; all-input-idle clock/expiry semantics remain pending. Full external
33-field profile/session parity, bounded rankings, legacy state_* removal,
combined capacity/backfill/slow-consumer/fault and actual 24-hour gates remain open.
These other tickets are not this ticket's current work. STR-44 is post-MVP.

Previous detailed ledger is preserved at
`target/str38-acceptance/status-before-str38.md`; historical validation is in
[milestone-3-validation.md](milestone-3-validation.md). Task breakdown:
[milestone-3-work-plan.md](milestone-3-work-plan.md).
