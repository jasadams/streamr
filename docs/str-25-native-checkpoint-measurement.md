# Native checkpoint measurement — 2026-10-29

**First experiment: INCONCLUSIVE against the frozen acceptance gates.**
The mechanism has substantial observed CPU/checkpoint savings, all declared
output oracles pass, and native reuse/compaction evidence passes. Some baseline
phases lack the required two wholly-contained completed checkpoints. Do not promote these
observations into a fully accepted performance or throughput-capacity claim.

## Source and controls

Base `8cddb321b631b79185bf6d166a0fdf9522db529f`; complete candidate compiled-source
manifest SHA `02744b92d97e656ae94d8e4ea9599b18d8fe7b662a044edf771d198f9bc8cf08`.
All 1,354 candidate inputs match the implementation worktree. Baseline has 1,349
inputs: 1,347 exact base files and the two reviewed measurement overlay files.

- Baseline executable `09f9db9ee9c1cfb568a3cbbadc3b55e9aa84e4ab4360c5554fa20f35ab6abd47`.
- Native executable `869ae908845557e301c887959b7ef41f95ac4c7927949e964c386b2870d0ea8e`.
- Both optimized/test profiles link verified precompiled RocksDB archive
  `cd8300d81598fb5e0e6b5dd73b55e1355fc004b7f4ca8b27341cf759ef927d54`.
- Frozen manifest `3181aaf81345e0f905638c3892bc224d5cbd9561cfb86a154f8f49365c93edbe`.

One ABBA comparison used four physical cores (0–3), singleton ownership,
controller-mode publication, 32 MiB backend pools, a 512 MiB sampled child-RSS
limit and five-second delay after checkpoint completion. Each arm had fixed
seed/idle/sparse/churn boundaries at 0/60/80/100/120 seconds. Total processing was
480.045579 seconds. All four arms completed all 59,474 offered rows and phase
input targets with exact full/prefix output oracles. Input payloads and every
mutation/lookup result were independently reconstructed. No extra drain or
second experiment occurred.

The workload seeds 49,152 independent 8,192-byte payloads (384 MiB logical),
updates 492 keys, then deletes/reinserts 4,915 keys. One final input sentinel is
not released. Same test-only pacing source, SQL, queue behavior and limits apply
to both roles. This is a generic native MERGE/lookup fixture, not the earlier
Flink workload, a production Kafka benchmark or a whole-host memory bound.

## Observed cost

CPU and transfer totals below are means across the two arms of each role.
Checkpoint medians pool only wholly-contained checkpoints in the named phase.

| Measurement | Full export | Native | Observation |
| --- | ---: | ---: | --- |
| Sampled process CPU per arm | 69.845 s | 30.170 s | 56.8% lower |
| Idle sampled CPU per arm | 8.640 s | 0.485 s | 94.4% lower |
| Sparse sampled CPU per arm | 9.165 s | 0.780 s | 91.5% lower |
| Idle checkpoint request→publication median | 4.029 s | 0.106 s | 3 / 8 completed samples |
| Sparse checkpoint request→publication median | 4.159 s | 0.106 s | 3 / 8 completed samples |
| Reported completed-checkpoint transfer per arm | 2.541 GB | 0.688 GB | 72.9% lower |
| Sampled process physical writes per arm | 4.379 GB | 2.369 GB | 45.9% lower |

Reported transfer sums newly written checkpoint payload files, including global
tables in both roles. Native reused file references are excluded from this
reported-byte counter; the full referenced inventory is a separate measurement.
The counter excludes protobuf metadata and unfinished partial checkpoint work,
and does not measure provider request/copy overhead. Process write counters
include database/compaction and checkpoint writes; they are not network traffic.
GB above is decimal. Sampling can miss final CPU/I/O and RSS peaks.
Native opt-in boundary/property receipts add candidate-only cost; no overhead
was subtracted.

Native initial checkpoint tails of 5.45/5.35 seconds and churn compaction
checkpoints around 1.68 seconds with roughly 219.91 MB newly uploaded SSTs remain
in the evidence. These observations do not claim every native checkpoint is
faster. Fixed offered work makes 59,474/120 approximately equal in all arms;
there is no demonstrated throughput-capacity multiplier.

Native selected state is about 503.3 MB, above the 32 MiB pools and observed
process RSS (native peaks about 228/231 MiB). The filesystem was warm and kernel
page cache is outside that process measurement. This establishes larger-than-
backend-resident live/checkpoint state. The separate [fresh-recovery gate](str-25-native-checkpoint-validation.md#large-state-fresh-recovery--2026-10-29)
passes all 49,152 retained-key SQL probes on normal dev/test and optimized pins.
Those whole-process checks are correctness evidence, not another comparison.

## Physical reuse and compaction

Independent audit rehashed 780 unique native payloads and checked 42 CURRENT→
MANIFEST relationships. Complete manifests preserve unchanged SST paths and
verified file identities; private metadata is newly uploaded. Idle counters stay
unchanged. In both native churn arms, raw pinned-version internal stats show
seven additional L5 compaction counts, increasing read/write totals and key
removal, with installed replacement inventories corroborating completed rewrite.
Counts alone are insufficient: the pinned source records some stats before its
final status check. Raw displayed GB units are GiB and rounded; do not infer
exact rewritten bytes from those rounded columns.

## Unmet gate and decision

| Arm | Idle completed CPs | Sparse completed CPs | Churn completed CPs |
| --- | ---: | ---: | ---: |
| Baseline 1 | 2 | **1** | **1** |
| Native 2 | 4 | 4 | 3 |
| Native 3 | 4 | 4 | 3 |
| Baseline 4 | **1** | 2 | 2 |

The frozen requirement is at least two in **every** phase of **every** arm.
Cross-boundary checkpoints stay excluded. The raw result remains INCONCLUSIVE;
native compaction audit clears its own placeholder, not the baseline gap.
Technical review supports retaining the mechanism while leaving full measurement
acceptance unresolved. User decision on the [bounded phase-length follow-up](str-25-checkpoint-measurement-followup.md)
is pending; no follow-up implementation/run is authorized yet.

The sampled frontier-lag metric begins at observed completed output and ends at
publication. It is not event-admission latency or Kafka committed visibility.
Independent source/Kafka pending-commit recovery evidence is recorded separately
in [validation](str-25-native-checkpoint-validation.md).

Raw evidence and independent reports:
`/home/jason/qa-evidence/str25-native-checkpoints-20261028/optimized-abba/`,
`performance-oracle-process-audit.{md,json}`,
`optimized-native-independent-review.{md,json}`,
`optimized-checkpoints-independent.csv`. Source/native/profile preflights and
shared-target cache-recovery receipts are preserved alongside them.
