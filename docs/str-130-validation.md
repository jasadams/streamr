# STR-130 compact aggregate codec validation

Source base: `0e898c82c0ac4081c9e8f50d3aa981eb30bbf941` (merged STR-129).
Evidence: `/home/jason/qa-evidence/str130-compact-codec-20261015/`.

## Change and decisions

`GroupCodec` stores ordinary scalar fields directly with schema-bound identity,
validity bits and exact-width/length-delimited payloads. Primitive records do not
construct Arrow arrays or IPC messages. Complex fields use a cached Arrow subset
schema and the pinned Arrow conversions; generated messages append directly into
the final value without an additional full-body staging copy. Lifecycle state,
accumulators and optional previous output remain one atomic backend value.

SQL kernels, CDC equality/order, backend keys, commits, checkpoints, configuration
and resource pools are unchanged. The obsolete group format is removed; no
compatibility reader is retained. Calendar progress/cleanup formats are unchanged.
No new or changed RocksDB scan is introduced.

Three independent pre-code reviews approved PLAN revision 2 and resource
amendment 3A. Code review requested and verified constructor, framing and targeted
corruption-test repairs before approval. Hash receipts bind these reports to the
production files. Scope was narrowed from an unsupported comprehensive Arrow
workspace proof to contract-preserving complex serialization and explicit new
direct ownership checks.

The direct decoder checks encoded bytes plus new scalar-vector/payload ownership
against three times the configured value limit before allocation. A 262-case
same-input comparison found 22 previously accepted wide all-Null records now
refused by that check. The user explicitly approved retaining this stricter
decoded-memory enforcement. Example: 384 Null fields at B = 8,192 encode to 6,492 bytes
with the old codec and 113 bytes compactly, but compact decode refuses its scalar
ownership beyond 24,576 bytes. Spare-capacity strings and sliced complex values
remain accepted. Original failed sweep evidence is preserved.

Complex Arrow allocation behavior retains its existing limitations; this change
does not establish a universal 3B bound for complex ownership or an entire worker.
It neither raises limits nor adds a new admission system.

## Production checks

Normal dev/test profile; pinned Bookworm image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`;
matching precompiled RocksDB archives; maintained runners and shared queue.

| Gate | Result |
| --- | --- |
| Locked worker Cargo check | PASS, exit 0 |
| Strict worker Clippy, all features/targets | PASS, exit 0 |
| Formatting | PASS, exit 0 |
| Codec tests | 17 passed, 0 failed |
| Incremental aggregate/calendar tests | 75 passed, 0 failed; 5 opt-in diagnostics ignored |

Exact commands/exits are in `run-production-checks.sh`, `final-gate-exits.tsv` and
their logs; `production-gate-binding.json` records source hashes. Initial compiler
and fixture failures are retained separately, not counted as passing checks.

Coverage includes exact scalar types/bits/metadata, complex/null/dictionary/union
values, malformed framing/reader hazards, bounded direct refusal and successful
retry, lifecycle/CDC and actual native export/restore into fresh memory/RocksDB
owners. The strengthened collection fixture persists a mixed List/timestamp
previous output, releases the original owner before restoration and verifies exact
retract/append CDC plus duplicate suppression. These are production physical
operator tests in one process, not OS-process-loss or full Engine qualification.

## Performance evidence

The saved STR-129 AFTER is reused unchanged as BEFORE: 294,912 minimum completed
outer-handler events / 600.086942 seconds = 491.4488 events/s. It is instrumented
four-owner progress, not durable checkpoint or final sink completion.

Isolated representative development-profile measurements observed scalar
2,284 → 155 bytes and calendar 5,100 → 221 bytes; encode allocation calls 131 → 1 and 273 → 1,
decode 58 → 2 and 124 → 2. These are measured byte/allocation changes, not optimized
whole-engine throughput results. The four-case codec comparison and acceptance sweep
are evidence-only overlays; no baseline codec or allocator enters production.

The optimized isolated codec comparison passed both selected tests, including the
262-case acceptance sweep with exactly 22 approved refusals and no unexpected
differences. Representative scalar encode/decode means were 10.249/5.140 µs for
IPC versus 0.329/0.424 µs compact; calendar 22.516/12.190 µs versus 0.678/0.754 µs.
These ABBA measurements compare synthetic fixed shapes with the same thread-local
allocation instrumentation; they do not predict a whole-worker multiplier.
Requested allocation peaks exclude allocator fragmentation and physical realloc
overlap. Exact arms and allocation counts are retained in
`codec-benchmark-release.log` and its summary JSON.

The reviewed release pin uses the same immutable image and actual host/runtime
native archive closure as saved STR-129. Every compiled source outside the six
approved codec/private fixture files matches the 596-input baseline; approved
production-to-probe reconstruction and pin hashes are retained. Candidate binary
SHA256 is `cc27ee76ae07b68e6ca92ef508c1732fa61240354cbc93fdef0be4d34f1fa714`.

| Bounded instrumented worker evidence | Saved BEFORE | AFTER |
| --- | ---: | ---: |
| Minimum completed four-owner handler rows | 294,912 | 385,024 |
| Observed processing seconds | 600.086942 | 599.538967 |
| Completed handler rows/s | 491.4488 | 642.2001 |
| Matched own process CPU seconds | 1,630.69 | 1,701.62 |
| Matched average CPU cores | 2.7193 | 2.8401 |
| CPU µs / matched completed-handler progress | 5,529.41 | 4,419.52 |
| 100ms sampled owned-group RSS peak MiB | 392.0391 | 334.6797 |

The observed handler rate increased 30.67%; matched CPU per completed-handler
progress decreased 20.07%. AFTER completed scalar/calendar/SESSION/table handler
rows were 389,053 / 389,053 / 388,984 / 385,024. The conservative minimum is
conditional on no handler error, not durable progress, source-send rows or final
sink completion. CPU samples span 599.133022 seconds AFTER and 599.675888 BEFORE;
initial minimum progress is zero in both, and both boundary identities match.
RSS is the 100ms sampler peak including audit/cleanup, not the lower 1s metric
peak or exact physical maximum; maximum AFTER sample gap was 0.116434 seconds.

AFTER stopped without drain at 599.538967 observed processing seconds, within the
600-second cap. Both checkpoints reached ready (112.058 and 107.354 seconds;
BEFORE 140.374 and 160.820). These differ in retained state and progress and do
not establish matched-state checkpoint acceleration. The last approximately
61-second AFTER growth window observed 805.958 rows/s without checkpoint
transition; the approximately 60-second BEFORE window observed 542.045 rows/s
but overlapped a checkpoint transition. The last approximately 120-second windows
observed 682.472 versus 407.231 rows/s and both overlapped checkpoints. These
windows are descriptive cumulative-minimum differences, not an isolated matched
mechanism experiment or minimum per-owner interval delta.

Observed scalar/calendar state_encode inclusive wall/poll spans decreased from
5.387/10.652 seconds to 0.716/1.062 despite greater progress. Poll time is not
CPU; parent/child and concurrent stage times cannot be added. Native LOG counts
and emitted compaction-byte fields remain in comparison.json; stall-related log
lines include summaries and are not stall events or stall duration. No scan or
backend tuning changed. Greater retained progress and checkpoint/host-load
variation prevent an unqualified production speedup or I/O-amplification claim.

Offline exact observed-prefix oracles passed with no partial trailing JSON:
AFTER table 389,086 rows, scalar/calendar 303,104 each, SESSION 0; BEFORE table
303,027, scalar/calendar 245,760 each, SESSION 0. Persisted source clocks align
to the same UTC day. Neither arm Finished; neither establishes full 1.2M-source
completion, final-phase key coverage or SESSION EOF output. Output-file hashes,
raw counters, process CPU, timer, pin/control and cleanup receipts were independently
audited. Owned cleanup exited 0 with container absent, client joined and no
surviving owned PID/start identities; sampler joined without errors. No baseline
rerun or post-deadline checkpoint/drain was performed.

Evidence: `comparison.json`, `comparison.md`, `review-final-results.md`,
`after/{measurement-summary.json,offline-prefix-result.json,cost-analysis.json}`,
release-pin receipts and preserved raw samples/outputs. Keep this measured
increment on the finite experiment evidence; independent correctness/resource
and actual native recovery checks remain separate. Current-head PR CI and merge
are the remaining publication steps before choosing another hypothesis.

Full STR-103/126 qualification, OS-process recovery and Flink parity are not
established by this bounded experiment.
