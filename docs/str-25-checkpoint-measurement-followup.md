# Checkpoint coverage verification plan — completed

Status: completed following the user's direction to finish verification of the
same implementation. All coverage and native-compaction gates pass, with
independent KEEP; see [verification](str-25-native-checkpoint-verification.md).
The originally reviewed plan SHA `7c57dc116d92a9d1f65639cea394f77fe6d4e11effb86a9a51ce4a2e209475ff`
is preserved as `verification-preregistered-plan.md` in the evidence root.
The specification below records the bounded verification that ran. The first
frozen experiment remains unchanged and INCONCLUSIVE under its original gates.

## Evidence and remaining gap

The first ABBA campaign completed 59,474 exact outputs in every arm and all
state-change targets. Independent process/oracle audit reports mean sampled CPU
69.845 to 30.170 seconds and idle checkpoint request-to-publication median
4.029 to 0.106 seconds. This is fixed offered work, not a throughput-capacity
speedup. The original native compaction/inventory audit passed independently.

The fixed 20-second phases did not always contain two complete baseline
checkpoints: baseline 1 sparse/churn and baseline 4 idle each contain one. The
baseline maximum observed completed checkpoint duration was 4.9341 seconds;
its configured next-checkpoint delay is five seconds after completion. Phase
alignment can therefore require almost 24.803 seconds to contain two complete
checkpoints. This is a measurement coverage gap, not an output/recovery failure.

## Bounded verification specification

Keep the approved native-checkpoint hypothesis, product source, payloads, input
sequence, backend limits, source pacing, checkpoint policy, protocols, CPUs,
output oracles, diagnostics and all acceptance criteria unchanged.

Change only the measurement phase lengths:

| Phase | Original | Proposed |
| --- | --- | --- |
| Seed | 0–60 seconds | 0–60 seconds |
| Idle | 60–80 seconds | 60–88 seconds |
| Sparse updates | 80–100 seconds | 88–116 seconds |
| Delete/reinsert churn | 100–120 seconds | 116–144 seconds |

Use the same ABBA order: four 144-second arms, at most 576 processing seconds.
This leaves 24 seconds below the workflow's ten-minute processing cap. Continue
to require at least two checkpoints wholly contained in every measured phase
of every arm; do not reclassify boundary-crossing checkpoints. Stop the campaign
if the unchanged seed target is incomplete at 60 seconds. No input shrinking,
resource increase, interval change, post-deadline drain or automatic repeat.

Preserve 49,152 independent 8,192-byte seed payloads (384 MiB logical payload),
492 sparse updates and 4,915 deletes plus reinserts; final measured input target
59,474 and the unconsumed sentinel stay unchanged. Keep four physical cores
0,1,2,3, 32 MiB backend pools, 512 MiB sampled child RSS envelope, singleton
ownership, controller protocol and five-second checkpoint delay. Sampled RSS
is not an OS-enforced whole-host memory bound.

For this verification, update only the shared measurement overlay and matching
Python phase/audit constants. Copy the Rust driver byte-for-byte into both
source trees and obtain fresh independent overlay and plan reviews. Rebuild
both optimized SQL-test executables through maintained scripts, invalidating
only the changed first-party artifacts when switching worktrees. Reuse matching
precompiled native archives and the canonical target. Verify that compiled
product inputs match the accepted first experiment except for measurement-only
files; preserve new source/archive/executable receipts and a new campaign root.

Freeze the Rust test's whole-invocation deadline at 174 seconds and the
supervisor's startup/cleanup deadline at 184 seconds (the original 30/40-second
margins above processing). The Rust processing deadline remains exactly 144
seconds; preserve the original 0.25-second timer tolerance and cumulative
576.25-second receipt guard. No post-deadline drain or extra checkpoint counts.

Checkpoint retention stays min_epoch=0 with no measurement-time GC. The extra
eight seconds per phase can retain additional full baseline snapshots, private
native metadata, and compaction rewrites. Keep the existing 40 GiB absolute
workload free-space prerequisite and the machine queue's 100 GiB reserve. Audit
actual free space after accounting for retained first-campaign evidence,
new pins and the canonical shared target before startup. These are preflight
guards, not a global disk reservation or total storage bound. Fail closed on
insufficient space; do not remove earlier evidence or loosen resource settings.
Preserve owned runtime cleanup and exact artifact inventories after the run.

On every baseline/candidate source-tree switch, invalidate all changed first-
party product outputs and RPC generation through maintained cargo-dev targeted
package cleaning; touching only the driver or proto is insufficient. Preserve
third-party caches/native archives/shared target. Require fresh:false for every
changed compiled product artifact and native/RPC fingerprint/source closure
verification before accepting a pin. Reuse the independently reviewed product
source; only paired measurement overlays and corresponding audit constants
may differ from the first experiment.

Require exact full/prefix oracles, all phase targets, minimum checkpoint
coverage, bounded resources and verified completed compaction/rewrite evidence.
Report CPU and checkpoint/transfer costs for fixed completed work. Label sampled
output-observation-to-publication lag accurately; it is neither event-admission
latency nor Kafka committed visibility.

All three plan perspectives approved the timing specification before
implementation; paired source overlays, build orchestration and actual frozen
pins/fixtures received fresh independent review before processing. The user
directed completion of this same-hypothesis coverage verification. No production
rollout, extra performance hypothesis or larger qualification campaign is implied.
