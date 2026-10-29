# Native checkpoint sprint validation

Native implementation, independent source review, strict Clippy and current-source
dev SQL recovery gates pass. Source-free CLI worker/controller process-loss and
isolated Kafka pending-commit replay gates pass with independent evidence audits.
The completed [coverage verification](str-25-native-checkpoint-verification.md)
passes every phase-count and native-compaction gate with independent KEEP.
The first optimized ABBA comparison remains INCONCLUSIVE against its original
sample-count gates; its evidence and boundaries are unchanged.

Evidence root: `/home/jason/qa-evidence/str25-native-checkpoints-20261028/`.

## Plan review

Base `8cddb321b631b79185bf6d166a0fdf9522db529f`; user architecture approval on
2026-10-28. [Plan v2](str-25-native-checkpoint-plan.md) SHA-256
`0ab019cc00f336b90fa73ec70252d5125a47fde0503c5ca2edecb9443b3e862c` is preserved
as `plan-v2.md` under the evidence root.

- `lifecycle_review`: APPROVE, no blocking findings. Native capture and restore
  preserve the state cut, current cross-backend behavior and managed resources.
  Native reader cleanup must finish before final memory construction at a
  one-database limit; missing confirmed cache metadata means fresh upload.
- `shared_gc_design`: APPROVE, no blocking findings. V1's unresolved aggregate
  reachability memory bound was repaired with explicit external sort/merge and
  disk quotas. Validate roots and finish candidate spooling before deletion;
  retain manifests on partial payload-deletion errors for retry.
- `mutation_upstream`: APPROVE, no blocking findings. V1's unspecified phase
  timing, assumed compaction and Kafka latency denominator were repaired with
  fixed deadlines and explicit coverage/measurement definitions.

All three reviewers conclude this exercises the intended native capability,
bypasses no SQL behavior and stays within the user's architecture approval.
These are pre-code plan verdicts; they do not substitute for the independent
implementation and raw-evidence reviews recorded below. No signatures are
required or used.

## Work ownership

The coordinator owns TableManager, five native operator construction sites,
shared test-helper integration, default metadata/barrier literal updates,
documentation, the build queue and final evidence audit. Native backend work
owns capture/upload/restore and storage transfer. Protocol work owns schema,
completion propagation, validation and both cleanup paths. Test work owns
dedicated real-engine native checkpoint cases and finite runners. All share
the single STR-25 worktree; builds remain serialized.

## Delivery and qualification limits

Fresh-process restore of the measured native state passes on the existing normal
dev/test pin and on the optimized producer pin, with every retained key checked
through real SQL. The state exceeds configured resident pools and observed
sampled process RSS; it does not exceed whole-host RAM. The user directed
completion of the same performance verification; all four matched arms and
independent evidence audits now pass. The first result remains immutable.
[Draft PR #90](https://github.com/jasadams/streamr/pull/90) is published.
PR checks are limited to active Lint and Backlog tooling workflows;
full CI and Semgrep are manually disabled, so those jobs have not run.
The maintained local Rust gates above remain distinct from remote CI.

Current-source dev SQL gates verify six epochs and 48 fresh-process restores,
all five native backend construction paths, same-backend zero logical scans,
and three-page native-to-memory recovery with max-open-databases=1. Actual CLI
worker/controller loss and Kafka pending-commit replay also pass. Cloud ownership
and cleanup cases use mocks; live cloud qualification is not claimed. Process-loss
runs retain three old working-database attempts totaling 155,251 logical bytes;
no native capture directories remain. Global orphan scavenging is outside this
change.

The following sections preserve the investigation chronology. Later passing gates
supersede earlier failed or pending checks without removing their evidence.

## Integrated source review and initial checks

Independent cross-reviews found and repaired canonical-collision rollback
ownership, exact selected-epoch restore binding, hidden local multipart files
after process death, delayed completion handling, inference of a ready epoch
from an unrelated start threshold, uncertain orphan frontiers, and synchronous
GC spool work on an async reactor. GC now uses leveled merges to avoid repeatedly
rewriting old large runs. The reviewers require runtime evidence after these
repairs; source approval alone establishes no recovery or performance result.

`gate2-state-check.log` and `gate2-worker-check.log` record successful maintained
locked dev/prebuilt checks. `gate2-protocol-tests.log` records a test-compilation
failure: a mock wrapper called an inherent storage method instead of the
ProtocolStore method. The author repaired that resolution; at that stage, the
unit suite had not run. `build-failures.md` retains earlier infrastructure
and compiler failures, including the overwritten first state-check raw log.

The matching maintained image is
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Initial checks use dev/test and the matching prebuilt RocksDB archive. Release
builds are reserved for the reviewed optimized comparison.

The disk blocker is cleared: user-approved cleanup removed 21 unused STR-127
cold-build cache volumes and 44 historical STR-109/112 state/checkpoint
directories, recovering about 17 GiB and leaving about 120 GiB free. Reports,
logs, scripts, binaries and raw outputs were preserved and verified. Exact
retirement and reference audit: `approved-cleanup-receipt.json` and
`retired-state-file-inventory.jsonl.gz` in the evidence root. The maintained
`gate3` dev/test reservation was the next current-source check at that stage;
its results are recorded below. No further architecture decision was needed.

## Cloud transfer contracts

Local native uploads use visible, exclusively created temporary files, so a
killed process cannot hide SST payload from ordinary fenced enumeration. Cloud
uploads persist a bounded provider/temporary-key/upload-ID journal before the
first payload part. GC validates exact temporary ownership and retains the
journal on abort failure. Atomic destination creation records positive ownership
before rollback may delete the destination.

S3 and R2 use direct conditional CopyObject headers instead of the pinned SDK's
opaque multipart-copy promotion. AWS added destination conditions to CopyObject
in October 2025; the service's single-copy size ceiling is checked before upload.
An owned small-object collision probe rejects unsupported S3-compatible behavior
before transferring SSTs. See [AWS conditional copy announcement](https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations/)
and [R2 conditional copy extension](https://developers.cloudflare.com/r2/api/s3/extensions/).

Azure's empty multipart ID is legitimate. Its adapter keeps provider-specific
journals and clears uncommitted blocks only at the fenced unique temporary key
with an empty Put Blob, then deletion. Because Copy Blob may acknowledge a
pending operation, the source must stay present until destination block-blob
contents are readable; a bounded one-byte GET supplies that completion proof.
See [Microsoft block-blob lifecycle](https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs)
and [Copy Blob completion semantics](https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob).

These are source-reviewed adapter contracts, not live-service qualification.
Unknown upload IDs created before durable journaling carry zero SST payload, but
unidentified registrations and late in-flight parts still depend on provider
lifecycle guarantees. An abort acknowledgement alone is not proof that all remote
parts have been reclaimed. No cloud-wide hard storage cap or AWS/R2/GCS/Azure
process-loss qualification is claimed. Mock-provider tests and actual local
process-loss tests must be distinguished in the final evidence.

## First current-source runtime gates

`gate3c` dev/test with matching prebuilt native library passes four native backend
cases (complete SST reuse/deletion/restore and max-one-DB portability, corrupt
restore/memory failure cleanup, positive destination ownership, blocked remote
cleanup), cancellation admission, three controller-native GC cases including the
missing-root repair, the exact older-epoch restore-binding guard, and five
streaming-storage cases. The storage provider cases include local subprocess
exit and mocked cloud transfer, not live cloud service qualification.

`gate3-protocol-tests.log` records 67 passes and three failures, retained intact.
The missing pruned-parent parser matched the manifest basename rather than the
epoch directory, breaking cleanup retries. The ancestry bound returned a generic
error instead of the existing typed cycle error. Both are repaired with preserved
assertions and an explicit repeated-cutoff shared-SST regression; the full-suite
rerun is recorded below. `gate3b-native-tests.log` records a test-compilation failure from a
nonexistent TaskInfo field in the new guard fixture; the literal was corrected
before the passing `gate3c` run. At that stage worker completion and SQL executable
checks were in progress; no lifecycle/performance result had been established.

`gate4-protocol-tests.log` now passes the full 70-test protocol library suite,
including all three previously failing cases. `gate4-cleanup-tests.log` passes
all three controller-native cleanup tests after final formatting. The delayed
native completion worker regression passes in `gate3c-completion-test.log`.
The SQL test executable built and was preserved in gate3c; gate4 is recompiling
and repinning against the final formatted source before runtime matrices.
Independent source and paired measurement overlay approvals are preserved as
`native-source-review.json`, `protocol-source-review.json`,
`integration-source-review.json` and `native-measurement-overlay-review.json`.
These verdicts do not claim runtime recovery or performance success.

## SQL recovery and post-review validation

`combined-lifecycle-result.json` binds the exact gate4 executable
`2b4afdd946da1f039aeaf87f794d777ffc641e0f2db9567393a07a9ea1d607e8` to
controller cases from gate4 and leader cases from gate6: 24 captured epochs and
48 fresh-process cross-backend restores pass exact literal output/file checks.
`gate6-state-table-lifecycle.log` passes all eight current configurations;
`gate6-window.log` passes four TUMBLE/HOP cases; `gate6-session.log` passes two
SESSION cases; `gate6-joins.log` passes eight updating-join configurations.
These existing operator fixtures reconstruct workers within their documented
scopes; the separate new lifecycle runner establishes actual fresh OS processes.

Gate4/gate5 monitoring failures are preserved: Linux revoked `/proc/PID/io`
access during exit before `waitpid` was ready, after successful Rust execution.
The monitor now waits at most one 100ms sampling interval and requires confirmed
termination; a live denial remains fatal. Actual short-subprocess fault checks
pass in `monitor-exit-race-check/result.json`, including live-child cleanup.
The final monitor repair is independently approved in integration review.

Controller compilation passes (`gate6-controller-check.log`). Strict Clippy
then found enlarged internal completion enums. Both completion payloads now use
owned boxes; seven narrow producer/consumer files preserve the same wire and
completion contract, with one heap allocation per completion and compact queues.
Independent review: `boxed-checkpoint-completions-review.json`.
Post-boxing joint state/worker/controller/SQL checks pass
(`gate7-controller-check.log`). Two subsequent cleanup style warnings are fixed.
At that stage `gate8` was running the strict gate, final executable freeze and
runtime matrices; its outcome was pending. Prior evidence is not attributed to
this later layout repair. A new 40-key restore diagnostic is independently
reviewed to measure actual multi-page native-to-memory traversal and zero-scan
same-backend restore; it had not run yet. Process-loss/Kafka and performance
were also pending then.

## Final dev gates and measured scan work

`gate10-clippy.log` passes the maintained strict all-features/all-targets gate for
all ten affected crates. Current-source SQL pin SHA-256
`4901fc31f284ee283531cd777dc3be86d299b89e15049d8a48e5997499889efa`
is bound to unchanged before/after inventories of 1,354 compiled inputs.
The joint worker pin independently passes the delayed-completion regression.
`gate10-lifecycle/result.json` passes all four combinations and 48 fresh-process
restores. Existing state-table eight configurations, four TUMBLE/HOP, two SESSION
and eight updating-join cases pass again on that final executable.

`gate10-restore-scan-diagnostic/result.json` passes a new 40-key capture with
four deletions and four updates, then exact continuation from epoch 6 to 52
outputs. RocksDB restore emits zero logical scan pages. Native-to-memory restore
materializes all 36 retained keys on one paired native snapshot in three pages
(max 16 entries / 131,072 requested bytes each): 3 Seek, 38 Next, 40 valid
candidate visits, 46 internal-key skips and 4 deleted-key skips. Native block
reads total 12 / 12,048 bytes; these are RocksDB block counters, not device I/O.
Seek starts at namespace/cursor, no native lower bound, exclusive namespace-
successor upper bound. Snapshot release is logged; the one-database restore succeeds, and reviewed
construction order closes that reader before memory DB admission. No exact
reservation-byte or snapshot-duration telemetry is claimed. Enumeration discovers all
arbitrary retained keys; known-key reads cannot materialize the destination.

Whole-process diagnostic CPU is 0.152144s Rocks / 0.159542s memory, sampled RSS
145,534,976 / 145,899,520 bytes. Those include startup, restore, three resumed
inputs and shutdown, with opt-in profiling overhead; they are not production
rates or restore-only times. Physical read deltas were zero on the sampled warm
filesystem. Small retained state verifies bounds/progress and actual traversal;
At this dev gate, large-state/performance and real process loss were still
separate pending gates.
Fetched origin/main still equals the recorded base before CLI packaging.

## CLI process loss and Kafka replay

Maintained dev CLI pin `dev-cli-pin` completed with matching native archive,
actual embedded UI assets, source/native/runtime receipts and source-free smoke.
Binary SHA `d9a4ffa9471ab0d8bbce6ae10bc16380bf72537e8b1234cb6d928ce54ed10fab`.
Three real OS-process-loss cases pass exact 6,000-row sequences and aggregate
CDC continuity to count 6,000 / sum 17,997,000: worker loss in controller and
worker protocols, and controller loss with durable catalog identity preserved.
The first wrapper used `--rm`, so Podman removed its CID file before the wrapper
could verify cleanup. The failed outer receipt remains; independent absence
verification and successful helper cleanup reconcile that bookkeeping failure.
The two continuation cases use explicit CID cleanup with zero exits.

`kafka-native-before-1` passes the existing isolated `before` recovery branch:
65 exact committed records per sink, original physical prefix retained, selected
committing checkpoint replayed by a replacement worker. Outer container and
broker-volume absence checks pass. The established isolated transaction timeout
is 900,000ms; this correctness control is not a production setting change or
performance improvement. Independent raw Kafka/native inventory audit passes:
selected epoch 3 has complete typed namespaces and inherits an SST born at
epoch 1 through base 2; post-recovery epoch 4 preserves lineage and reuses
older files through base 3. Source offset 2, both prepared transaction identities
and all 65 mutation/lookup outputs were independently decoded or reconstructed.
See `kafka-native-evidence-review.md/.json` in the evidence root. This pending
commit case did not require replaying already-checkpointed source records.
No production service was touched.

Hard-kill scratch inspection retains the existing documented old working-DB
orphan limitation. These gates do not claim cross-attempt local-root reclamation
or full larger-than-RAM milestone qualification. Native remote/upload lifecycle,
operator recovery and owned test-process/container cleanup are distinct facts.
The optimized comparison subsequently completed through reviewed same-queue
orchestration. Its source, accounting, observed savings and remaining frozen
coverage gap are recorded in [measurement](str-25-native-checkpoint-measurement.md).

## Large-state fresh recovery — 2026-10-29

The measured native checkpoint 21 (saved source offset 59,474) was cloned
read-only and restored into an empty managed RocksDB directory on real Btrfs
storage. The unchanged optimized candidate then produced 49,152 new native SQL
point-lookup results, covering every retained key, with an exact 108,626-row
full oracle including the preserved historical sink prefix. This is actual
engine recovery; Python prepares input/expected output and does not rebuild
backend state.

Independent audit `large-native-recovery-result-review.{md,json}` passes:
503,303,110 native file bytes and 402,653,184 logical payload bytes exceed
303,198,208 bytes peak sampled child RSS. All 182 process samples confirm
CPU affinity 0–3. Independently verified configuration retains 32 MiB backend
pools and max-open-databases=1. Enabled logical-scan diagnostics record zero pages. The source-free
container has read-only checkpoint/input/binary mounts and a fresh disk-backed
working directory. Original/clone hashes are unchanged; the owned container is
absent and no working database files remain.

Whole-child elapsed 18.266649s and CPU 21.222040s include startup, restore,
historical source-prefix skipping, probes and teardown; these are not
restore-only latency or another throughput comparison. Sampled physical I/O
deltas are read 342,233,088B and write 525,504,512B, with warm-cache/sampling
limits. The 512 MiB RSS envelope is sampled, not an OS-enforced or whole-host
RAM bound.

The first launch failed before engine invocation because rootless OCI lacks
the cpuset controller. Its original evidence remains under
`large-native-recovery/`. The successful separately prepared `retry1` uses
the established inherited Linux affinity mechanism and verifies the actual
child allocation. This does not establish post-SIGKILL working-DB scavenging.

The required normal dev/test large-state gate also passes with the unchanged
gate10 executable `4901fc31f284ee283531cd777dc3be86d299b89e15049d8a48e5997499889efa`
and actual matching dev archive
`c1c76537d9d655fca93b9cc5e8f50f82406ef5da584294aee013605f69d5bd3e`.
All 1,354 compiled inputs equal the optimized producer's source inventory.
Native provenance is the retained unique Cargo native artifact/build-script
dev linker selection plus the previously audited image archive receipt; this
does not assert a new final-SQL fingerprint closure. No build was repeated.

The dev run restored the same epoch/source offset into a separate empty disk
directory and passed the same 49,152 new probes and exact 108,626-row oracle.
Peak sampled RSS is 380,051,456B, below logical payload 402,653,184B and native
files 503,303,110B, with unchanged 32 MiB pools, `max_open_databases=1` and actual affinity 0–3.
All 1,216 process samples retain that affinity; diagnostics show zero logical
scan pages. Elapsed time of 122.181700s and waited child CPU time of 142.541041s include the whole
process lifecycle and are not restore-only or a performance comparison. Warm
cache sampled physical I/O deltas are read 1,441,792B/write 525,299,712B. One
confirmed child-exit sampling race was handled by the existing bounded monitor;
no live-process sampling denial was ignored. Host checks pass original/clone
hashes, exact output and owned cleanup. Independent audit
`large-native-recovery-dev-result-review.{md,json}` passes the scoped normal
dev large-state recovery gate.
No engine source or resource limit changed. Evidence: `large-native-recovery-dev/`.

## Completed coverage verification

The user directed finishing the same approved implementation's verification.
Only paired measurement timing changed; product source, input/oracle bytes,
CPU/backend limits and checkpoint policy are unchanged. Four 144s ABBA arms
completed 576.030329162s total, all 59,474 exact rows per arm and at least two
wholly-contained checkpoints in every measured phase. Independent native audit
checked 824 payloads/52 manifest links and successful local SST replacement in
both native arms. Independent process/oracle and storage verdicts support KEEP.

Whole-window sampled CPU means 78.705→30.820s (60.8% lower), idle checkpoint
median 3.978→0.110s. No throughput-capacity or production Kafka latency claim.
The original first campaign remains unchanged and INCONCLUSIVE; raw verification
JSON retains its reserved independent-audit status, resolved in the separate
written reviews. [Exact current evidence and limits](str-25-native-checkpoint-verification.md)
include source/pin hashes, phase counts, SST/compaction proof, CPU scope and
resource/cleanup qualifications. No additional engineering hypothesis was tested.
