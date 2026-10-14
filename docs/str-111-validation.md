# STR-111 calendar reuse and pruning

Status: implementation, independent source review, isolated operator diagnostic,
delivery worker/SQL checks, shared cancellation regressions and bounded production
comparison and independent final evidence/source review complete. Publication
and current-head CI remain pending. Full integrated qualification remains with STR-103/126.

## Ownership and source

Recovered existing branch `jason/str-111-calendar-20261014` in
`/home/jason/repos/streamr-worktrees/str-111-calendar-20261014` under actual session
`01a139e5-ceab-74f3-94bc-ae45176dead6`. The original claimant rollout
`01a139ca-e322-7690-9a29-4615c5956a09` ended with `turn_aborted` at
2026-10-14T09:45:08.490Z, immediately after pickup. The user confirmed there was no
STR-111 agent. Original reservation metadata was preserved before atomic takeover;
no previous implementation, PR or remote branch existed. STR-112 remains owned by
its original worker.

Original base `6793409cb53eb7b13f5a591ce1717bcf1e846de8`; existing clean delivery
branch refreshed onto fetched main `fa9c8133d3feefb4343e322c5405e843e25e1b61`.
Read current `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md` and the full STR-111
ticket/comments. `CLAUDE.md` and `docs/CODING_STANDARDS.md` are absent.

Independent diagnostic arms both start from frozen base `6793409c`, before the
STR-110 codec and STR-112 exporter changes. This base contains native scan bounds,
first-entry scan reduction and operation telemetry in both arms. Detached trees
`str111-pilot-baseline-20261014` and `str111-pilot-candidate-20261014` preserve this
isolation: baseline adds only the identical ignored diagnostic; candidate adds
only this ticket's changes/tests. The main delivery diff adds no codec/exporter
changes. Evidence resides outside Cargo caches at
`/home/jason/qa-evidence/str111-calendar-20261014/`.

## Established contract and changes

The existing native calendar implementation is a fork addition; upstream Arroyo's
ordinary aggregate accumulator machinery supplies COUNT/SUM. Existing persisted
G/Q/B/J/W/V/H/U ownership, typed accumulator merging and checkpoint paths remain
the contract. Existing operation-amplification and matched stack-attribution
reviews were read rather than rerun. They show repeated calendar fanout, without
establishing that calendar recalculation dominates CPU or caused the historical
7 events/second result.

- G's decoded horizon accumulator can be reused only for a live existing generation,
  matching persisted per-aggregate Q reference, and an original contribution that
  is absent/static-filtered, has no contribution day, or lies outside that exact
  inclusive horizon. Corrections use J's original gate/day/value. Relevant changes
  always reconstruct in the same ascending bucket order; floating SUM is not
  reassociated. Relevant rows do not incur an additional Q read.
- An identical persisted U deadline leaves the existing H/U pair intact. A changed
  deadline retains the established delete/insert/pointer update in one scope.
- Horizon scans supply an exclusive native upper bound at reference+1, in addition
  to the existing group/generation/family prefix and lower seek. Overlay entries
  obey the same endpoint. The generic store's admission calculation includes the
  additional endpoint; existing first_from callers retain their behavior.
- Pruning visits expired buckets as before. On the first retained day in one
  group/generation/family, it persists that family's maximum-day key as V's exclusive
  frontier. All later days in that same family are retained, so subsequent pages
  and cold workers can skip them. No G/J history or lifetime state is removed.

## Calendar scan audit

| Scan | Need, bounds and frequency | Progress and remaining cost |
| --- | --- | --- |
| Horizon B scan | Merge typed day states for the exact group/generation/storage family, from reference−horizon+1 through reference inclusive. Runs for changed reference or relevant original contribution; irrelevant rows with matching Q now skip it. The maintenance callback still recalculates on explicit day advancement. Native exclusive upper bound prevents reading later days merely to discover the stop. | One first-entry request per merged bucket plus termination; each page/cursor is released within existing bounds. The existing inclusive lower seek may reread the previous key. Sparse/deleted history still requires RocksDB traversal within native bounds; this change does not claim to remove internal tombstone cost. |
| Pruning B scan | Global B prefix is necessary because calendar keys order group/generation/family before day. Each real advancing watermark, native flush and native startup resumes at most one admitted page; completed V performs only W/V point reads. Synthetic raw references, Idle and EOF never authorize deletion. | W/V are checked, and V advances atomically with deletions. Page capacity reserves the V operation/value. Expired buckets still cost one logical visit each, including internal deleted-history traversal; retained suffixes now cost one family visit rather than one per day. New watermark day restarts V for stricter floors. |
| Maintenance callback / H-U registration | Per-row H/U deadline registration and the existing recalculate_calendar_group callback remain. Source audit found callback callers only in tests; its dead-code annotation assigns production scheduler integration to STR-29. No production H traversal is proved here. | Callback G/dirty updates and normal retract-before-append output remain unchanged. Equal per-row reschedules avoid rewriting H/U. Callback tests do not establish native quiet-key scheduling; no new scheduler is added under STR-111. |

No new scan, timer service, admission pool, checkpoint format or backend-specific
SQL execution path was introduced.

## Focused validation and finite pilot

The first standalone test command was cancelled while still waiting for the
machine build slot (exit130), to put build, artifact pinning and dependent runs
inside one shared queue reservation. No compiler/test result came from it.
`run-pilot.py` now owns the serialized diagnostic build/pin/run reservation and
records commands/exits in `pilot-commands.json`. Existing STR-112 work is preserved.

The explicit ignored `calendar_operation_count_pilot` uses a fresh Rocks backend,
one native operator and 128 deterministic rows: 32 each static-filtered, old,
future (32 distinct days) and relevant, all at the same reference after one seed. The seed is excluded
from operation deltas. Two pairs use AB then BA ordering. Exact final lifetime/day/
week values must be 129/33/33. Existing native operation histogram sample counts
measure invocations, not physical keys/bytes or CPU. Timing includes expression,
scope and commit work; it is an operator diagnostic, not graph steady processing,
durable throughput or production qualification. A separate phase times real watermark
pruning through persisted V completion, including point-read/scope observation
overhead, with a predeclared 64-page bound. This phase reports its native-operation
deltas and page count separately from event processing.

Both pinned isolated builds passed (baseline291.94s, candidate26.86s); the shared
diagnostic source hash is
`e99318040a518cbea8a2a968215e43a683b1ae64bac587c5b4724d6710ecac9b`
in all three trees, verified again after queue acquisition. Baseline executable
SHA256 `8d86dee6acf5340310ff011684b4d8b0f7e9c1bc7bbce8b1e8fd73ff7300f77c`;
calendar-only candidate `723c995e7b086971d85c7206d21915dac65fe948f79ea11eaf06bef99d71e8a1`.
See `baseline-pin.json`, `candidate-pin.json`, execution-time source receipt,
`pilot-commands.json`, raw pair logs and `pilot-summary.json` in the evidence root.
All four explicit ignored-test invocations passed both exact value and full
retained-bucket oracles.

| Pair (order) | Baseline event seconds | Candidate event seconds | Baseline pruning seconds | Candidate pruning seconds |
| --- | ---: | ---: | ---: | ---: |
| 1 (baseline, candidate) | 0.844481431 | 0.690136118 | 0.029766311 | 0.006557340 |
| 2 (candidate, baseline) | 0.512704912 | 0.381068542 | 0.015029685 | 0.002032339 |

In each pair, event scan invocations fell512→128 (4→1 per event,75% fewer).
Read invocations stayed1088 (8.5/event), snapshot128 and write-batch128; no read
or write-batch reduction is claimed for this event mix. Q/scheduler record writes
are avoided by source control flow, but these metrics count batches rather than
individual records. Pruning scanned35→3, read28→4, snapshot19→3 and wrote10→2
batches, completing9→1 bounded pages. Its counts include V-observer scopes and
reads in both arms. Native horizon upper-bound physical key traversal was not
separately counted, so no isolated speedup is assigned to that part.

The event rate ratios are1.224× and1.345× over two short O0 pairs. Absolute
timings vary substantially between pairs; these prove reduced operation fanout
on this finite mix, not stable production throughput or a general speedup. No
durable checkpoint comparison or continuous production RSS campaign ran.

Isolated candidate focused tests passed14 calendar scenarios (plus the one
intentionally ignored diagnostic) and3 aggregate-store tests. This includes
existing memory/Rocks checkpoint/fresh-owner continuation, NULL COUNT/SUM and
resource-failure cases. Delivery-source results are listed separately below.

New differential regression compares persisted horizon outputs to full bucket
recalculation after original corrections, both horizon edges, static false/null
filters and reference changes in both directions. New pruning regression verifies
persisted retained-family skip survives a cold owner. Bounded-store assertions
cover committed and pending overlay endpoints. Existing calendar COUNT/SUM null/
correction, interleaved-group, quiet-key callback, resource failure and memory/Rocks
checkpoint/fresh-owner recovery scenarios remain covered by the selected tests.

Independent reviewer `/root/scan_audit` approved the production diff with no
findings and separately approved the ignored pilot's input/count/value claims.
The reviewer also freshly approved the relevant-path optimization (avoid Q reads
when rebuilding anyway) and report, with no findings. The reviewer freshly approved the separate pruning diagnostic and its
post-timing exact retained-bucket oracle after fixing the initial missing bucket
assertion. Final measured evidence requires independent review before publication.

Current delivery-source locked worker check, strict all-target Clippy, formatting
and focused calendar14/store3 tests passed. Native SQL calendar/filter16 and
admission/pruning8 scenarios also passed: memory/Rocks, controller/leader checkpoint
protocol modes, batches1/8, exact CDC images and saved checkpoint continuation
into a fresh local Engine/task owner. These are local harness recovery checks,
not OS worker restarts or external coordinator failover. SQL test executable
SHA256 `37f80bf27788bcab34083787f5ad883a2d920b91a33638f990795278cae3d5eb`;
`sql-pin.json` binds357 source hashes to the delivery base/patch. Exact command
exits/durations: build40.759s, calendar-filter16 cases435.146s and pruning8
cases360.299s, all exit0 (`sql-commands.json` and named logs).

Independent reviewer `/root/scan_audit` approved these results after rerunning all
24 saved CDC/recovery oracles and84 batch1 prefix snapshots and checking the
binary/source hashes. The local modes do not establish production quiet-key
scheduler integration. Existing input-scope cancellation and slow-collector output-budget cancellation
tests both passed against the current pinned worker executable, exit0 in
`cancellation-commands.json` and their exact-test logs. Independent review approved
these as shared native aggregate resource regressions using an ordinary memory
aggregate; they do not establish calendar scan/prune cancellation or RocksDB
process-loss recovery. The worker pin records base and binary hash; source binding
is corroborated by the SQL357-source receipt and build cwd/trace, without a complete
compiled-source inventory for the worker-test executable.
The first diagnostic used the mutable `arroyo-dev` tag with a separately captured
image receipt; an immutable image at execution was not independently recorded for
each of those prior runs. SQL and subsequent production/cancellation commands bind
immutable image `e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.

The user explicitly replaced the historical four large 400,000-key/three-checkpoint
comparisons with a bounded production comparison plus focused correctness/recovery
checks on2026-10-14. The bounded optimized production comparison is recorded
below, separately from the O0 diagnostic. Independent final evidence review, PR and
current-head CI also remain. Full combined capacity, sustained durable throughput
and process-loss qualification stay with STR-103/126 and existing STR-104.

The first production candidate build was deliberately interrupted after detecting
redundant runtime/host RocksDB C++ compilation: the cache lacked completed native
archives/metadata, while the preserved outside-cache runtime archive exactly
matched baseline SHA256
`e6d88cd20f12949d474c40fb9a3b91a9f9f1cc0b018d129a16e712bb97ca4194`.
Its exit137, owned-container interruption and successful private-DB cleanup are
preserved. An independently reviewed pinner uses upstream external-static archive
linkage and fresh locked bindings in a new evidence root. Runtime native bytes
remain baseline O3; host Rust opt0 tools use that same native O3 archive instead
of the baseline host-only native O0 archive. This build-only difference is explicit;
no old/new generated-bindings byte identity is claimed. No production arm ran
during that interrupted attempt. The final observer captures CURRENT/OWNERSHIP
while the engine is alive, because normal cleanup removes live state.

The replacement optimized candidate CLI built/pinned successfully in734.003s,
SHA256 `40d11c0ee111aad20e1253f8cce2943f086fb3475a568b72a5c7255e340ef64f`.
Independent review verified all605 compiled source hashes,92 preserved native
closure/bindings proof hashes, package, immutable-image loader check and runtime/
host linkage separation before execution. Runtime LZ4 also matches the preserved
baseline archive hash. The first production pair failed harness assumptions:
metrics polling asserted after the owned worker listener had closed, then the
next arm's socket probe rejected the prior connection's TIME_WAIT. It supplies no
performance result. Both arms cleaned up with container absence, joined clients
and zero owned survivors. The failed pair is preserved under
`production-compare/failed-pair-1`. Independently reviewed harness corrections skip
unavailable owned-listener samples, permit normal socket reuse, and distinguish
controller-layout StateOwner generation0 from worker attempt generation1. Strict
Finished, source/downstream8192, exact output, native plan and ownership checks
were retained for the fresh pair; the approved binary was reused.

## Bounded optimized production comparison

One finite baseline/candidate pair passed with identical frozen8192-row input,
128keys, SQL, explicit reference-time watermark, original codec/exporter, pools,
batch32, page64, checkpoint120s and sink. The saved production plan in each SQLite
catalog contains one native calendar owner with eight COUNT/SUM descriptors for
1/7/30/90days. Complete CDC replay yields exactly128keys, lifetime COUNT/SUM64
and every horizon COUNT/SUM16. Source and native aggregate received counters are
exactly8192 in both arms; both jobs reached Finished without restart. Runtime
CURRENT/OWNERSHIP receipts bind job, exact operator and controller-layout state
owner generation0 separately from worker attempt generation1.

| Measurement | Baseline | Calendar-only candidate |
| --- | ---: | ---: |
| Observed Running to Finished seconds | 12.258126984 | 6.767395130 |
| Completed input contributions/second | 668.291 | 1210.510 |
| Submission to Finished upper interval seconds | 12.856535690 | 7.391622172 |
| Startup observed upper seconds | 0.868875497 | 0.842124326 |
| Poll uncertainty seconds | 0.106853249 | 0.111297589 |
| Peak sampled owned process RSS bytes | 223481856 | 222928896 |
| Completed checkpoints | 0 | 0 |

The single observed rate ratio is1.81135×. Timing includes EOF drain and controller
reporting; it is an observed completion interval with polling uncertainty, not an
isolated CPU measurement. Both sampled RSS values are below the declared512MiB
bound; sampling is not an exact maximum. Both inner cleanup and host cleanup
passed, with absent containers, joined clients and zero owned survivors. No
sustained-rate, durable checkpoint-rate, process-loss recovery or integrated
qualification claim follows from this bounded pair.

`production-compare/comparison.json`, per-arm audit/result/observations/metrics,
SQLite programs, exact output and host cleanup receipts preserve the outcomes;
`runner-attempt-5.log` and `commands.json` record the successful execution. Candidate
CLI40d11c0e was reused after source/package/build receipt verification, with both
arms using immutable imagee67afacf. Queue waiting, native build, interrupted
attempts and observer review time are excluded from the reported run intervals.

Independent reviewer `/root/scan_audit` approved the actual bounded pair after
replaying complete CDC oracles, auditing all57 baseline/31 candidate metrics
captures, both catalogs and Finished/run1 records, frozen controls/fixtures and
cleanup receipts. The reviewer independently confirmed rates and single-pair
ratio; no findings. Final source/report publication review also approved with no remaining findings;
reviewed staged diff SHA256
`caff5426649ba7d2d079a36d2eb7a2cc059a50d484e02dbd67cfc8fecc7acee0`.
