# Milestone 3 — current working ledger

Read this first when resuming. Historical evidence belongs in
[milestone-3-validation.md](milestone-3-validation.md); manageable tasks are in
[milestone-3-work-plan.md](milestone-3-work-plan.md).

## Current task and next action

User steering: proceed with manageable tasks. Active task: publish the reviewed
Kafka fixture-readiness repair and verify fresh PR #6 CI. The preceding reviewed
increment is published as `65827b2622f4c8acb6c9e1de31fcf58a1c64ead7`.
Its pull-request workflow `37705609443` passed (709 library tests and 12
integration tests); duplicate push workflow `37705605554` failed a Kafka
metadata test output timeout. Overall checks on that head are not green.

The narrow repair checks each topic-creation result and waits for exact
partition/leader metadata readiness before starting the reader. Production
behavior, topic/group IDs, output deadlines and value assertions are unchanged.
Independent source review confirms the applied file matches the reviewed
candidate. Both unchanged baselines passed all 11 Kafka tests; the CI timeout
was not reproduced locally, and precise causation remains inference.

Repaired-source validation passed: formatting, affected all-target/all-feature
check, strict Clippy, test build and 11 separate-process parallel Kafka tests
with zero retries. Logs, source and executable hashes, baseline/parser-error
receipts and reviews are in
`target/milestone-3-pr6-publication/kafka-ci-diagnosis/`. Check required a distinct
native build-script dependency profile; Clippy and final test build reused their
respective native caches. All runs are terminal; no build is active.

Next: publish this three-file increment after final evidence review, then wait
for all current-head CI checks. Keep PR #6 draft and M3 tickets open. After green
CI, seal the reviewed source reconciliation and run one prepared RocksDB/leader
hot-key SESSION capacity case with independent retained-checkpoint proof.
Preparation: `target/native-session-large-v15-preflight/`. The test-only change
is excluded from the pinned SQL/production artifacts; conditional reuse review
requires exact final diff/head, complete new inventory and fresh CI. Original
build receipts must remain unchanged. No capacity case has run yet.

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
