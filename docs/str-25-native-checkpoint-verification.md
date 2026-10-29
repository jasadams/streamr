# Native checkpoint coverage verification — 2026-10-29

**KEEP the reviewed native implementation.** Independent process/oracle and storage
reviews pass. Every measured phase of every arm contains at least two complete
checkpoints; actual native SST replacement corroborates successful compaction.
The first experiment remains unchanged and INCONCLUSIVE under its original
coverage gates. This completes verification of the same approved implementation
and performance claim; it introduces no new product optimization.

## Source and matched controls

The product is unchanged from PR #90. Relative to each role's first accepted
compiled-source inventory, only the paired measurement driver changed. Both
Rust drivers are byte-identical; all nine changed first-party libraries compiled
by this fixture rebuilt with `fresh:false` after targeted package cleaning.
Controller outputs were also invalidated. Exact source/native fingerprint closure
and the actual frozen workload received independent preflight approval.

- Baseline base: `8cddb321b631b79185bf6d166a0fdf9522db529f`.
- Candidate build HEAD: `49ef2d69f660431a050d84a05fa9a50f2da37b34`, plus the reviewed timing overlay.
- Baseline executable: `ce7b4d31762eb33fc485b8e6084c20ee2d4ffec2c7efdb67f4b081017c949d9e`.
- Native executable: `fdfe2255f447f1a2d72f3f27c1543d0b5438ad9df0ac9c908da88ba700ce2e2a`.
- Both release/test opt3 builds link matching precompiled native archive
  `cd8300d81598fb5e0e6b5dd73b55e1355fc004b7f4ca8b27341cf759ef927d54`.
- Frozen manifest: `02465083b7a638de54bad768311e788334516713e1fc39cb3e023175cd5e29c5`.
- Reviewed original plan: `7c57dc116d92a9d1f65639cea394f77fe6d4e11effb86a9a51ce4a2e209475ff`,
  preserved as `verification-preregistered-plan.md` in the evidence root.

Four ABBA arms used four distinct physical cores 0–3, controller mode, singleton
ownership, 32MiB backend pools, max-open-databases=1, 512MiB sampled child-RSS
limit and five-second delay after checkpoint completion. Input and expected-output
files are byte-for-byte identical to the independently validated first fixture:
49,152 independent 8,192-byte payloads (384 MiB logical), 492 sparse updates and
4,915 deletes plus reinserts, 59,474 measured rows and one unreleased sentinel.

Only observation windows changed: seed 0–60s, idle 60–88s, sparse 88–116s and
churn 116–144s. Whole Rust invocation/supervisor bounds are 174/184s. Total
processing was 576.030329162s, below the 576.25s receipt guard; there was no
post-deadline drain or extra checkpoint wait. Longer phases change average
phase input rates and checkpoint opportunities. Compare baseline/native within
this verification; do not compare old/new campaign totals as a product speedup.

## Completed work and coverage

| Arm | Exact completed rows | Idle CPs | Sparse CPs | Churn CPs |
| --- | ---: | ---: | ---: | ---: |
| Baseline1 |59,474 |3 |2 |2 |
| Native2 |59,474 |5 |6 |5 |
| Native3 |59,474 |5 |6 |5 |
| Baseline4 |59,474 |2 |2 |2 |

All input targets, literal full/prefix oracles and all 12 phase/arm coverage
cells pass. Boundary-crossing checkpoints remain excluded. Both raw result JSONs
retain their original statuses; the independent storage verdict resolves the
current runner's reserved compaction-audit placeholders in a separate report.
It does not alter or clear the first experiment's actual coverage failure.

## Measured costs

CPU, transfer and physical writes are means per arm; checkpoint medians pool
wholly-contained completions in the named phase. RSS is the maximum sampled
child peak across the two arms of each role.

| Measurement | Full export | Native | Change |
| --- | ---: | ---: | ---: |
| Whole processing-window sampled CPU |78.705s |30.820s |60.8% lower |
| Whole-window CPU per completed row |1,323.351µs |518.210µs |60.8% lower |
| Idle checkpoint request→publication median |3.978s |0.110s |5/10 samples |
| Sparse checkpoint request→publication median |4.050s |0.102s |4/12 samples |
| Completed-checkpoint reported payload |3.072GB |0.692GB |77.5% lower |
| Sampled process physical writes |4.991GB |2.375GB |52.4% lower |
| Maximum sampled child RSS |239.12MiB |229.74MiB |Both below 512 MiB |

GB is decimal; MiB is binary. CPU includes checkpoint, idle, background compaction
and candidate-only opt-in property receipts; no overhead is subtracted. The
CPU/row normalization is not exclusive row-transform CPU. Reported payload is
newly uploaded state/native files plus globals, excluding reused inventory,
protobuf/provider overhead and unfinished partial transfers. Physical I/O
includes database and checkpoint work; it is not network traffic. Sampling can
miss final intervals and peaks. Maximum observed sampling gaps are about 103ms.

Native first-upload tails of 5.30/5.45s and churn maximum 1.735s remain in the
evidence: not every native checkpoint is faster. Fixed offered work gives
59,474/144≈413 DATA/s for every arm; this is not a throughput-capacity claim.
Observed-output-frontier→publication lag is separately recorded and is neither
event-admission latency nor production Kafka committed visibility.

## Physical mechanism, recovery and limits

The storage reviewer independently hashed 824 unique native payloads and checked
52 CURRENT→MANIFEST relationships. Idle SST handles are retained; mutable native
metadata is private per checkpoint. Both native churn arms show seven additional
L5 compaction counts, rounded read/write increases of 0.5 GiB and dropped keys
0→4,298. Published epoch 22 inventories retire 176,798,271 bytes of local SSTs and
install about 219.93 MB of new local SSTs. Local filenames/checksums and installed
replacement inventories corroborate successful rewrite; counters alone do not.
Rounded raw cfstats GB headings actually represent GiB.

[Existing recovery evidence](str-25-native-checkpoint-validation.md#large-state-fresh-recovery--2026-10-29)
remains applicable to the unchanged product: 48 fresh-process cross-backend
restores, CLI worker/controller loss, Kafka pending-commit replay and separate
all 49,152-key native fresh recovery on normal dev/test and optimized pins.
No engine/codec/resource policy changed for this verification.

This is one local physical file-source MERGE/lookup fixture. Filesystem cache and
external host load can drift despite ABBA; cores are allocated, not exclusive.
Process RSS excludes kernel page cache and is sampled, not OS-enforced. Native
state exceeds the configured resident pools and observed process RSS; this is
not whole-host RAM exhaustion. Live cloud qualification and automatic post-SIGKILL
working-DB scavenging remain outside this evidence. Active PR checks are Lint
and Backlog tooling; broader CI/Semgrep workflows are manually disabled.

## Exact evidence

Root: `/home/jason/qa-evidence/str25-native-checkpoints-20261028/`.

- `verification-abba/`: immutable raw results, all sampled histories and checkpoints.
- `verification-oracle-process-audit.{md,json}`, `verification-progress-history.csv`:
  independent exact output, denominator, CPU, stalls, sample gaps and coverage audit.
- `verification-native-independent-review.{md,json}`, native audit and checkpoint CSV:
  independent reuse, accounting, manifest and completed-rewrite verdict.
- `verification-*-preflight-review.json`, paired overlay/orchestration reviews and
  `pins/release/verification-{baseline,candidate}/`: actual source/profile/native gates.

The original `optimized-abba/result.json` and manifest hashes are unchanged.
The one pre-build failure was a strict baseline source guard rejecting an old
Python bytecode cache. Its receipt is retained; only that generated cache was
removed, with no guard relaxation and no processing attempt consumed.
