# STR-115: logical progress while draining aggregate TTL

Base: `fa9c8133d3feefb4343e322c5405e843e25e1b61`, freshly fetched by the coordinator;
branch `jason/str-115-ttl-20261014`. Applicable instructions:
`AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, and
`docs/build-qa-troubleshooting.md`. No automatic elapsed limit or review signing.
Raw receipts and immutable test binaries are preserved outside the Cargo target in
`/home/jason/qa-evidence/str115-ttl-20261014/`.

## Contract and change

Upstream Arroyo's [updating aggregate](https://github.com/ArroyoSystems/arroyo/blob/18c203dcdd5515ab36237b2f02721570d6f20eef/crates/arroyo-worker/src/arrow/incremental_aggregator.rs)
materializes pending changes before timing out quiet groups. The fork's native
path preserves that order, samples `SystemTime::now()` for every expiry chunk,
checks current deadline equality before advancing generation, and deletes stale
index entries. Expiry and subsequent keyed dirty emission have separate generation
transitions; cleanup eventually removes empty keyed tombstones. Those semantics
and all configured limits remain unchanged.

The previous expiry function reset its logical lower bound at every committed
chunk. It now optionally retains one admitted logical E key within a serial flush,
advancing it only after the chunk commits. Input cannot interleave with that flush;
dirty emission only deletes E entries. Each chunk still creates its own stable
snapshot. No snapshot cursor crosses snapshots and no progress is serialized.
Error, cancellation, the next flush, and fresh recovery discard this local progress,
so an earlier insertion on a later invocation is considered. The reservation ends
before cleanup.

`AggregateStore::try_expiry_progress` uses the existing decoded pool. Four maximum
logical keys plus headers cover native Vec growth, cloning, replacement and capacity
normalization. The configured-capacity check leaves room for the existing scope,
dirty-output reservation and a maximum point read. Refusal falls back to the original
fresh-seek path, retaining no uncharged key. This follows the existing optional
SESSION cache contract in `session_store.rs`; it preserves configuration admissibility,
not success equivalence under arbitrary concurrent shared-pool pressure. The optional
bytes are charged and can contribute to an ordinary fail-fast refusal under contention.
No allocator, backend resource cap, execution admission or checkpoint protocol changes.

## Scan accounting

The E scan discovers unknown group keys ordered by expiry deadline; a point read
cannot discover those keys. G state is still read by point lookup. The scan retains
the E prefix and existing native exclusive prefix upper bound, and supplies the last
committed logical key as its inclusive native lower bound; `first_from` filters equal
keys to maintain exclusive logical progress. There is one seek per requested logical
entry, plus exhaustion/future checks. Returned pages and write chunks remain bounded
by the same page/write/overlay limits. Snapshot and page permits remain local to their
original scope/page; the only additional retained data is the optional admitted key.
No backend production scan implementation changes are proposed.

## Predeclared measurement

Before production edits, the finite existing native test machinery was extended
with a temporary ignored probe: 128 then 512 RocksDB groups, 192 then 768 due E
entries, four entries per committed chunk (48 then 192 chunks). Half the groups
have an obsolete deadline; every group also has its current deadline. Every group
must advance from generation 7 to 8 exactly once. This represents growing deletion
history and stale-index churn; actual interleaved native-event updates are separately
covered by the correctness regression.

Both arms use identical input, clock contract, test profile, backend pool/page/write
settings and no collector/sink. Temporary PerfContext code runs in the actual native
scan thread and records seek calls, deleted-entry skips and internal-key skips.
`/proc/self/status`, `stat`, and `io` are preserved at early/middle/end points.
Measurement rates count completed E-entry retirement, including stale records;
they do not count completed changelog output or source ingestion.

The dev/test profile and prebuilt RocksDB archive are unoptimized (native O0).
These timings diagnose this path and cannot establish production throughput,
larger-than-RAM performance, or full STR-103 qualification. Immutable image and
native archive receipts are in `baseline-receipt.json` and
`native-archive-receipt.txt`; full source patches accompany the pins.

Baseline production code was measured before the candidate edit. Baseline pin
SHA-256: `27be4c020cf42b1171e1a60db8a51966b66e2c5e77659843a811435cb5843bd5`.
Its initial 128-group drain retired 192 entries in 116.768 ms, with 4,512 deleted
and 5,277 internal-key skips. The 512-group drain retired 768 entries in 550.558 ms,
with 44,832 deleted and 45,789 internal-key skips. Native compaction can change
retained physical history during a run; measured counters are reported directly.
Candidate pin SHA-256:
`bd7d5ea5964a7d4648b1dceb3037e44ae5c8806566f6d0b43ad78f1aa3fdcc29`.
All six runs passed, including the identical generation assertions. The exact
execution order was initial baseline (B), initial candidate (C), repeat-1 B,
repeat-1 C, repeat-2 B, repeat-2 C: B, C, B, C, B, C. Each pair ran baseline first;
the pair order was not reversed or counterbalanced. All six raw logs, source
patches, image receipts and `probe-summary.json` are preserved.

| Groups / E entries | Seek calls per arm | Baseline deleted skips | Candidate deleted skips | Baseline internal skips | Candidate internal skips |
| --- | ---: | ---: | ---: | ---: | ---: |
| 128 / 192 | 192 | 4,512 | 47 | 5,277 | 812 |
| 512 / 768 | 768 | 44,304–44,832 | 191 | 45,789 | 3,260 |

Counts cover completed chunks; terminal exhaustion and post-drain integrity scans
remain in the raw log, outside these totals. Baseline 128-group drains took
109.394–384.033 ms (one large outlier), versus candidate 107.581–111.591 ms.
The 512-group drains took 540.441–576.251 ms versus 480.913–494.434 ms.
The repeated native work reduction is established; these short O0 timings do not
support a stable production speedup.

For the final baseline-first pair (repeat-2 B then repeat-2 C), each interval is
one third of the completed chunks: 64 E entries for 128 groups, 256 for 512 groups.

| Groups / arm | Early entries/s | Middle entries/s | Late entries/s | Early / middle / late deleted skips |
| --- | ---: | ---: | ---: | --- |
| 128 baseline | 2,478.1 | 1,493.2 | 1,572.3 | 480 / 1,504 / 2,528 |
| 128 candidate | 2,166.2 | 1,615.7 | 1,630.0 | 15 / 16 / 16 |
| 512 baseline | 2,174.2 | 1,327.4 | 1,113.8 | 8,064 / 24,448 / 11,792 |
| 512 candidate | 2,566.9 | 1,647.1 | 1,069.9 | 63 / 64 / 64 |

The candidate has a slightly lower late rate in the 512-group paired run, despite
far fewer E tombstone skips. No uniform chunk-latency or complete-drain collapse
repair is claimed. Remaining mutation/read/write work is outside this scan counter;
this ticket does not expand into neighboring cleanup or backend optimizations.
All other interval results are retained in `probe-summary.json`.

Sampled process high-water RSS was 93,600–98,040 KiB for baseline and
95,084–97,956 KiB for candidate; executable/dependency pages dominate these small
fixtures. CPU ticks and process I/O are preserved in `process-summary.json` and raw
logs, with the original sampling points. No memory reduction or capacity claim.

## Validation status

Diagnostic comparisons passed. The first instrumentation-free attempt passed locked
worker check, strict all-target Clippy, formatting and all three aggregate-store tests.
The aggregate module recorded 47 passing tests and one failing new recovery test,
then aborted with `pthread lock: Invalid argument` (SIGABRT); Cargo exited 101.
The complete failed attempt is preserved in `failed-final-attempt-1/`.

The failing tight memory fixture exported through a snapshot that does not expose
checkpoint resource configuration, so the established exporter requested its fixed
1 MiB page (4,229,780 bytes of scan admission) against a 1 MiB fixture scan pool.
Source tracing also found `checkpoint::ensure_empty` uses global worker resources;
these unit fixtures have none, so restore requests the established fixed 1 MiB page.
That independent requirement is incompatible with the tight Rocks fixture's 1 MiB
scan pool. The declared matrix therefore covers six combinations: ordinary/tight
expiry on memory and RocksDB, plus ordinary-budget partial-drain recovery on each
backend. Tight+checkpoint combinations are omitted and are not claimed passed.
Recovery uses existing normal checkpoint fixture pools; no exporter/global config
or backend changes, no production cap increases, and no increase to the tight
decoded budget that proves optional-progress refusal. Exact changelog oracles
remain unchanged. The failed container exited and was removed; its test state
was confined to disposable `/tmp`, and no owned live container remained. No claim
is made that the subsequent abort independently establishes a product regression.
The corrected full selected modules passed serially to isolate native lifecycle
teardown. The current-source inventory (`final-gates.json`) is:

| Command | Result |
| --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` | Exit 0 |
| `scripts/preflight-clippy.sh -p arroyo-worker` | Exit 0; locked, no-deps, all-features, all-targets, `-D warnings` |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib --no-fail-fast arrow::incremental_aggregator::tests -- --nocapture --test-threads=1` | Exit 0; 48 passed, 0 failed, 0 ignored; 1.65 s |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib --no-fail-fast arrow::aggregate_store::tests -- --nocapture --test-threads=1` | Exit 0; 3 passed, 0 failed, 0 ignored; 0.02 s |
| `scripts/cargo-dev fmt --all -- --check` | Exit 0 |
| `git diff --check` | Exit 0 |

Both new TTL tests passed. The exact changelog fixture completed all six declared
combinations, each with 12 native input events and 23 collected CDC rows (138 rows
total), no extra second-flush output, exact values and generation assertions.
Both memory and RocksDB partial-drain recovery used the existing controller
export/restore into fresh backend/operator instances. Tight expiry/refusal is
validated independently on both backends; omitted tight+checkpoint combinations
are not results. The corrected source was not separately rerun with default parallel
test scheduling; the earlier failure/abort remains recorded, and PR CI is a separate
gate. No abort occurred on the corrected serial run.

Temporary ignored probe and native PerfContext instrumentation were removed before
the production gates. `rocks.rs` is byte-identical to the frozen base. The final
instrumentation-free test executable is pinned outside the target, SHA-256
`a1974616e86d66b50e861624cbfba338af05006fb5ecb99c1b4141e20bf25c5f`.
`final-source-receipt.json` records source hashes and the unchanged immutable image;
`failed-fixture-cleanup-receipt.json` records cleanup of the failed attempt.
The focused regression covers exact dirty-update-before-TTL changelog sequences,
quiet and non-due keys, stale/equal deadlines, no repeated expiry, stage-specific
generations, cancellation before commit, failed admission/retry, earlier insertion,
optional progress refusal, and production controller checkpoint export/restore into
fresh backend/operator instances. Memory/RocksDB and ordinary/tight decoded budgets
are selected. This is fresh backend/operator recovery, not a process-loss campaign.

Preserved failed attempt: the initial diagnostic build exited 101 on a probe-only
Arc ownership error in cleanup; no baseline measurement resulted. The correction
uses the existing `Arc::try_unwrap` cleanup pattern. Its full compiler log and failed
source patch are preserved. The initial separately queued no-run command was cancelled
before starting (143), then replaced by a combined build/pin/run reservation.
Final independent source/evidence review approved publication. The reviewed
implementation is `a9039a16eaacc0d504526f381d7ef7db5de66ecb` in
[PR #48](https://github.com/jasadams/streamr/pull/48). All six reported CI checks
passed on that head: Rust formatting, Web UI lint and tooling for push/PR runs.
These active workflows do not execute Rust tests; the serial local results above
remain the executable validation evidence. This publication-record update changes
no tested executable source. The coordinator verifies final current-head CI before
the Trakkt green handoff; merge-sweeper owns merge. No deferrals; broader STR-103
qualification and server/browser batch QA remain separate.
