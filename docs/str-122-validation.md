# STR-122 updating join fanout validation

Measurement base `5f326ae533b6ba877031d7da314017f31f2d2113`, integrated onto
`2368459dacd53f9e3aa5024fb5a4f31ba96f8dab` with unchanged measured join hash
and no guidance changes (`integration-receipt.json`); branch
`jason/str-122-join-fanout-20261015`. Read current `AGENTS.md`,
`.claude/team.md`, `.claude/build-test.md`, milestone ledger/work plan, full
STR-122 and build troubleshooting. `CLAUDE.md`/`CODING_STANDARDS.md` are absent.
Evidence and immutable pins: `/home/jason/qa-evidence/str122-join-fanout-20261015/`.

## Established contract and change

STR-102 introduced this updating path (`682bea0d`). Existing upstream-style
`instant_join.rs`/`join_with_expiration.rs` execute DataFusion plans over matching
retained batches; they do not provide a different native updating implementation.
This repair preserves the existing STR-102 per-pair plan, bag identities, ordered
CDC, NULL behavior and first/last LEFT match transitions. Actual fanout remains.

A right-side LEFT change previously re-proved right-side existence/sole match
for every matching left row, in both deterministic preflight and real output.
The candidate caches only the resulting boolean, lazily on the first matching
left row, for this one change. Retraction needs one absence proof; insertion
needs first/next-key proofs when the inserted row is first. No-match/NULL/INNER
paths perform no additional proof scans. Match pagination remains unchanged.

The scope exclusively owns its overlay, staged before fanout. Both passes borrow
it immutably; `emit` and collector callbacks receive no scope reference and cannot
mutate its overlay. The stable snapshot does not change across awaits, including
if some external caller writes to the underlying backend. Commit follows both
passes. The boolean is a local on the change future, discarded on error,
cancellation or completion; it is never checkpointed or shared with later input.
No counts/indexes, public interfaces, resource limits or admission were added.

## Scan contract

All requests still use `AggregateScope::first/first_from`, including its stable
snapshot plus read-your-writes overlay merge. Logical row prefixes are
`R || side:u8 || join-key-length:u32be || complete-encoded-join-key`. The physical
lower bound is the greatest escaped namespace+prefix/start/cursor; the exclusive
native upper bound is the byte successor of the escaped namespace+prefix. These
bounds already existed. Point reads cannot enumerate unknown opposite identities
or prove no other matching right identity exists.

Opposite enumeration advances logical `after` strictly, discarding `key <= after`.
With page1 the inclusive native start revisits the prior key and may issue a
second seek. Existing page entry/byte limits, bounded overlay and snapshot remain;
this ticket does not claim that residual pagination cost was eliminated. Deletions
in the overlay are skipped, insertions merged in key order. Recovery starts a new
scope/snapshot and does not reuse any progress or proof from before recovery.

One scope/snapshot covers preflight, output and commit; each bounded decoded page
is released before another is requested. Right proof rows/keys are dropped after
the boolean is computed. The only added retained memory is `Option<bool>`.
Actual probe-row counting still increments once per logical opposite row, with
the same exact boundary and deterministic rollback before output.

## Finite measurement

Identical dev/test O0 builds and matching precompiled RocksDB, immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
The source-bound opt-in test uses real planned INNER/LEFT pair execution and both
backends, page1/32768-byte pages, 8192-byte values, fixed 1MiB cache/memtable/scan/
decoded/write pools and 8MiB memory backend ceiling. The finite fixture probe
limit is48, retained-row limit100, write65536/8 operations and overlay65536.
These test settings stay identical across arms; production configuration is unchanged.

Grow matching left state16→32→48, retain32 deleted lower right identities under
an identical pre-deletion snapshot, keep default compaction. At each stage run
four first-insert/duplicate-insert/nonlast-retract/last-retract cycles. Fully
collect and assert ordered flags, values, multiplicity and distinct pair identities
per change. Each stage completes16 target fanout changes plus four no-match/NULL controls,
and respectively256/512/768 INNER or
384/768/1152 LEFT output rows. NULL/no-match changes are asserted in the same stage.
The completed-change counter counts target fanout changes only; timing includes
the four controls. Exact limit48 success and47 refusal, unchanged identity after refusal, and
notification-proven cancellation during real output are checked on both backends.

PerfContext runs in each actual native scan task and reports seek/child/memtable,
internal/deleted/recent skip, block read/byte and seek/next-time counters. Per-stage
`/proc/self/stat`, `status`, `io` starts/ends preserve CPU ticks, I/O and RSS/HWM.
Native counters isolate backend work from SQL output count and sink cost.
Instrumented ABBA and dormant-probe ABBA timing arms are separate; neither is
optimized production throughput, larger-than-RAM capacity or full qualification.

All ten runs passed (baseline/candidate pilots plus eight ABBA arms),120 exact
stages. `measurement-audit.json` audits each stage, settings and all exits. Two
instrumented pairs give these actual Rocks LEFT counts (before→after; ranges
preserve native background variability):

| Live left rows | Opposite seeks | Right proof seeks | Internal skips | Deleted skips |
| --- | --- | --- | --- | --- |
| 16 | 1028→1028 | 1152→24 | 33152→2157 | 25312→531 |
| 32 | 2052→2052 | 2304→24 | 90144→3953 | 50944→536 |
| 48 | 3076→3076 | 3456→24 | 137056→5504 | 76416→536 |

Right proofs perform20 logical first/next lookups per16 target changes,24 native
page seeks, independent of matching left cardinality. INNER controls issue zero
right proof scans and identical opposite seeks1028/2052/3076 in both arms. No-match
controls contribute four of those seeks, zero proof scans and zero output; NULL
controls perform no scan. The residual64*n opposite-page cost is preserved.

Probe-disabled ABBA timings, outputs fully collected (rows/s ranges over the two
runs per arm; finite unoptimized test, not a production-speedup claim):

| Join | Left rows | Output rows/stage | Before rows/s | After rows/s |
| --- | --- | --- | --- | --- |
| INNER | 16 | 256 | 192.4–209.7 | 167.3–194.2 |
| INNER | 32 | 512 | 144.7–207.6 | 137.1–192.4 |
| INNER | 48 | 768 | 175.9–193.8 | 146.2–174.1 |
| LEFT | 16 | 384 | 109.1–175.4 | 177.7–210.2 |
| LEFT | 32 | 768 | 118.5–138.2 | 155.4–189.3 |
| LEFT | 48 | 1152 | 118.7–144.8 | 181.9–200.5 |

External unqueued Trakkt Cargo CPU activity overlapped the late campaign, as
confirmed by process elapsed times at the first final-queue rejection. Wall
timings are confounded; these are not isolated throughput measurements. INNER
timings are noisy and the candidate is slower in these controls despite identical
native work; do not infer a stable or general execution-throughput improvement.
LEFT reduces measured repeated native work and these finite output timings, with
unchanged output cardinality. Native child seeks/block reads/block bytes are zero
in all instrumented timed stages: this is a memtable/cache-scale workload, not SST
or disk-capacity qualification. Native memtable seek/internal traversal still
varies across snapshot versions/background activity; exact per-arm counters and
seek/next nanoseconds are retained in `analysis.json` and raw logs.

Probe-disabled LEFT process CPU ticks (user+system, kernel tick100Hz) for
16/32/48 rows: before206–246/452–494/672–727, after172–194/369–393/533–550.
Corresponding RSS ranges KiB: before150012–150788/150160–150908/150276–151024,
after149496–150428/149620–150564/149728–150684. These whole-process samples include
DataFusion, allocator and backend work, not just the state pool. Physical read
bytes are0 throughout; write bytes per stage before49152/53248–57344/53248,
after49152/53248/49152–53248. Full logical-I/O/syscall counters, RSS/HWM and CPU
starts/ends remain raw; instrumentation includes substantial logging overhead and
is kept separate from the dormant-probe timing arms.


## Verification and limits

Final combined reservation `b7d7e3a07e79` exited0 on integrated source, normal
matching precompiled dev/test profile. `final/receipt.json` and
`final-combined/results.json` preserve exact commands/exits; tracked source was
unchanged through SQL build/recovery. Only this report/ledger changed afterward.

| Selected check | Result |
| --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` | exit0,387.6s |
| `scripts/preflight-clippy.sh -p arroyo-worker` | exit0,79.6s |
| `scripts/cargo-dev fmt --all -- --check` | exit0,5.8s |
| Locked worker lib test build/pin, `--no-fail-fast --no-run` | exit0,250.4s |
| Locked worker lib `arrow::updating_join`, `--no-fail-fast --test-threads=1` | exit0,7 passed/0 failed/1 ignored |
| Explicit `native_join_fanout_growth_churn --ignored --nocapture --test-threads=1` | exit0,1 passed/0 failed,27.44s runtime |
| Locked SQL-testing lib build/pin, `--no-fail-fast --no-run` | exit0,136.4s |
| Existing direct INNER/LEFT memory/Rocks controller/batch1 fresh-worker SQL oracle | exit0,4/4 cases,16.7s |

The inventory covers dictionary NULLs, before-images, key changes, pair identity,
output/value limits, cancellation and nonempty/empty checkpoint restore to fresh
backends. The explicit pilot checks both backends/kinds, duplicate/churn output,
exact48/47 probe boundary, deterministic rollback and notification-proven real
output cancellation. No selected product test failed. All selected commands
completed before assessing results; no failure was omitted or reclassified.

Worker test pin:
`71059f8bc3d9ceaf0e02e5630a783e542ee8fb2580436d0c26d1a504ea6d5bf5`.
SQL test pin:
`66e4f024e912330b8cfad9fe78780a0877da2abe395facdc0d51f841a7106c09`.
Both remain outside Cargo caches. `final-audit.json` verifies measured/final join
source equality, probe-free state equality to integrated base and all four case
receipts. Selected source/proto timestamps were refreshed before gates to avoid
stale generated/shared-target artifacts; source contents were unchanged.


Initial `baseline/` preserves exit2 before compilation: local `arroyo-dev`
resolved to stale `e67afacf…`, rejected by maintained native pin preflight.
`baseline-matching-image/` uses the already-built matching immutable image;
no wrapper bypass or native source build was used. `final-queue-rejection.json`
and `final-queue-rejection-{2,3,4}.json` preserve four queue-startup
exit1 refusals before any final command: cleanup correctly deferred during unrelated
unqueued Cargo/compiler activity (new builds and an active compiler draining
through shared sccache after its Cargo parent exited). No unrelated process was stopped. Cache
maintenance used only the maintained queue.

Temporary state instrumentation is removed before final checks. Source receipts
bind measured worker code to final code and verify final `rocks.rs` equals base.
Current-source direct INNER/LEFT memory/Rocks controller/batch1 SQL recovery
passed the existing fixture driver and exact full-value/CDC/checkpoint-prefix
oracle. Committed/recovered row counts were INNER memory0/6, Rocks0/6; LEFT
memory2/15, Rocks6/21. Scheduling can change physical CDC counts; each complete
value, before-image, checkpoint prefix and final bag passed the existing oracle.
The existing harness aborts workers, waits for native cleanup, and constructs a new Engine from the
checkpoint; this is fresh-worker recovery, not OS process-loss qualification.
Independent final review, publication and current-head CI remain coordinator gates.
STR-103/126 retain full integrated capacity/recovery qualification.
