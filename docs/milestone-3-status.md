# Milestone 3 — current working ledger

Read this first when resuming. Historical evidence belongs in
[milestone-3-validation.md](milestone-3-validation.md); manageable tasks are in
[milestone-3-work-plan.md](milestone-3-work-plan.md).

## Current task and next action

User steering: proceed with manageable tasks. Active task: finish the serialized
v16 validation of the reviewed session-expiry snapshot optimization, publish it,
verify fresh PR #6 CI, then rerun the unchanged large queue32 SESSION case.
The Kafka fixture-readiness repair is already published and green. The preceding reviewed
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
respective native caches. Those Kafka validation runs are terminal; the later
v16 combined validation below is active.

Published repair: `77ca1517cc99bd596b81f742a4f4f78ebd4e602f`. Final independent
evidence review passed. Push/PR CI runs `37709508918` and `37709514364` both passed: each has 709
passing library tests, six skipped and 12 passing integration tests. All seven
current-head checks are green; watcher `88454` is terminal zero. The concrete
source-reconciliation seal and exact reviewed driver are activated; final
activation review passed. The one RocksDB/leader hot-key SESSION case failed;
root session `87793` is terminal one. SQL child failed after 0.69 seconds before
checkpointing: graph queue reservation needed 259.0 KB with 194.3 KB left in the
16 MiB execution pool. No capacity/recovery or actual-retained-state pass is
claimed. Preserve log `target/native-session-large-v15-preflight/one-case-driver-77ca1517.log`
and the full `case-77ca1517...-rocksdb-leader/` evidence. Read-only diagnosis is complete: default queue_size8192 can retain roughly
64.75 MiB of these batches per edge; fail-fast shared-pool admission is existing
tested behavior, with no leak or contract violation established. Prepare a
separate declared queue_size32 operational candidate using existing native
configuration after sanitation, preserving the failed case and all batch/input/
pool/RSS/timeout/value/retained-proof assertions. Independent review passed for the separate queue_size32 driver. That run
also failed: SQL capture exceeded its unchanged 900-second deadline after the
reference run and checkpoint-prefix processing, before recovery and actual
retained proof completed. Root session `27539` is terminal one, with no SQL
process left. Preserve `one-case-driver-77ca1517-queue32.log` and the distinct
`case-77ca1517...-rocksdb-leader-queue32/` directory. No capacity pass is claimed.

Active bounded repair: `repair_validation_preflight` owns only
`crates/arroyo-worker/src/arrow/session_store.rs` and inline tests for a constant-size,
conservative expiry proof that avoids snapshots when no session can be due.
`publication_review_coverage` independently reviews its contract and final diff.
Use existing strict expiry boundaries; Unknown after recovery; conservative
mutation/cancellation handling; no SQL/API/backend read-view/checkpoint change.
If those contracts cannot be preserved, discuss with the user before proceeding.
No new resource limits, deadline, batch, fixture or assertion changes are approved.
Source repair is ready and independently approved at SHA-256
`cf3f5d909fd901d35b5cd56664e6c59e86738eb149944c19dfdf6403de89bf02`,
with six new focused inline tests. Fresh v16 formatting, workspace all-target
check (15m34s) and strict Clippy (1m24s) passed. Library tests passed across 25
suites: 714 passed, zero failed, six ignored, including all six new expiry-proof
tests. The full all-target build passed (227.83 seconds). All 12 small recovery
integrations passed: hot8 SESSION, many4 SESSION and packaged unordered
operations across memory/RocksDB and controller/leader. Complete typed rows,
multiplicities and committed prefixes were checked. Exact current-source gate,
artifact and integration receipts are under
`target/native-m3-reviewed-repairs-v16-validation/run/`. Independent evidence
review passed with no blocking findings. It independently checked complete typed
outputs/prefixes and peak child RSS of 185,573,376–196,108,288 bytes (<512 MiB).
Report: `target/native-m3-reviewed-repairs-v16-validation/review-actual-evidence.md`.
This finite scope is not capacity proof.
`prepare_repair_runtime_smokes` prepared the combined runner/freeze in
`target/native-m3-reviewed-repairs-v16-validation/`; source seal is now true after independent runner approval.
The runner passed independent review before root sealed and launched its five
gates, artifact pinning and same 12 small integrations in one shared queue
reservation. Combined v16 validation root session `85738` is terminal zero;
foreground log `target/native-m3-reviewed-repairs-v16-validation/foreground.log`.
Fresh Cargo-selected SQL ELF SHA-256:
`c07a2391ab7a1545b97fa000e9483e56e09df0b9a5085864da6b7561df40ce78`;
production ELF: `74a23212802c544915b2f354f343802fe8f1bafda5af0e7e7f697cd508b400cc`.
Both are pinned outside Cargo caches. Green CI above qualifies only published
`77ca1517`, not this upcoming Rust change. Publish the reviewed repair and obtain
fresh exact-head CI, then finalize the inactive v16 capacity packet and rerun the
unchanged large queue32 case. Preserve both prior failed attempts.
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
