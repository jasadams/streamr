# Milestone 3 — current working ledger

Read this first when resuming. Historical evidence belongs in
[milestone-3-validation.md](milestone-3-validation.md); manageable tasks are in
[milestone-3-work-plan.md](milestone-3-work-plan.md).

## Current task and next action

User steering: proceed with manageable tasks. Active task: publish the reviewed
repair increment and verify PR #6 CI for the published head. The five fresh
Bookworm gates and all 12 small comparator integrations are terminal and passed.
No build or test is in flight. Do not restart completed validation without a
source change or contradictory evidence.

Next action: finish the final evidence review, commit only the reviewed batch,
push the existing PR branch, update its description and wait for current-head CI.
Keep PR #6 draft and M3 tickets open. Then execute one prepared RocksDB/leader
hot-key SESSION capacity case, with independent retained-checkpoint proof;
preparation is in `target/native-session-large-v15-preflight/`.

## Evidence and limits

- Independent native/execution/harness partition and repair reviews cover all
  changed code; integration coverage and reports are retained in
  `target/milestone-3-pr6-publication/`. Eight Python oracle tests passed twice.
- All five v15 gates passed: formatting, workspace all-target check, strict
  Clippy, 25 library suites (708 passed, zero failed, six ignored), full all-target
  build. Both startup delivery/cancellation regressions passed. Source, commands,
  exits and logs: `target/native-m3-reviewed-repairs-v15-build-evidence/`.
- Scheduled cache eviction removed the SQL-test executable after those gates.
  A successful unchanged-source rebuild (1190.44 seconds) then produced a new
  Cargo test artifact name; the old assumed name was rejected before smokes.
  Both fresh executables are now pinned outside Cargo caches. Original gate and
  rebuild receipts remain unchanged; do not substitute their executable hashes.
- All 12 small SESSION/operations cases passed using the verified rebuilt SQL
  ELF: complete initial/recovered typed rows and committed prefix values, both
  backends and protocols. Maximum child RSS was 195432448 bytes (<512 MiB).
  Evidence: `target/native-m3-reviewed-repairs-v15-runtime-smokes/`, including
  `integration-evidence.json`, `results.json`, logs and measurements. This is
  finite helper integration, not capacity, production-loss or full M3 parity.
- Preceding v14b: 40 finite state-table parity cases, four export faults, six
  production-loss cases and four producer/consumer pairs passed within their
  recorded older-source scope.
- PR #6 is draft; publication/CI must be tied to the new commit. The reviewed
  candidate is based on `b6b2aa7a`; upstream PR #4 base remains `fbd5179a`.
  Preserve unrelated untracked `docs/disk-backed-state-design.md`.

## Decisions and open gates

- No application schemas, policies or algorithms inside Streamr.
- Discuss observable semantics/interface or architectural changes before coding.
- Rolling zero/result composition remains unresolved: captured recent count 1
  versus required zero; updating-input JOIN is rejected. Encoding diagnosis is
  settled: Debezium timestamps use Unix milliseconds; do not rerun unchanged SQL.
- RocksDB read-view lifetime proposal remains unapproved.
- Optional per-group emission policy is STR-44, post-MVP.
- Full external identity/profile/session parity, bounded rankings, current-source
  ≥10×/hot-key/backfill/slow-consumer/recovery evidence, telemetry, legacy-function
  retirement prerequisites and actual 24-hour live/fault qualification remain open.
