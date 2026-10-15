# STR-119 duplicate occurrence retractions

Base `277adcb8a2ff8d2821530de4ba4fc2abd613db66`, freshly fetched by the
coordinator. Workspace
`/home/jason/repos/streamr-worktrees/str-119-duplicates-20261015`, branch
`jason/str-119-duplicates-20261015`. Read `AGENTS.md`,
`.claude/build-test.md`, `.claude/team.md`, current
`docs/milestone-3-status.md`/`docs/milestone-3-work-plan.md`,
`docs/build-qa-troubleshooting.md`, the full STR-119 ticket and
`/home/jason/.claude/agents/feature-implementation-engineer.md`.
Current user policy supersedes historical elapsed limits and review signing.
STR-118 retains SESSION ownership; this change does not touch it.

## Established contract and change

Upstream Arroyo revision `18c203dcdd5515ab36237b2f02721570d6f20eef` keeps
fallback row tuples in a counted HashMap. The disk occurrence index is a fork
addition. Its R keys contain group, generation, aggregate, length-delimited
complete encoded argument tuple and ordinal. Unknown-ID retraction removes the
lowest matching ordinal. Planner-injected timestamps already point-read R by
explicit stable row ID and then read its M member. These contracts are unchanged.
The preserved upstream file/hash and relevant lines are in `upstream-receipt.txt`.

One optional admitted logical last-deleted R key now belongs to a serial
`process_native_batch`. It transfers between existing committed chunk scopes;
no cursor survives the input batch. Exact full tuple-prefix mismatch is cold.
Every member append invalidates the lower bound before puts, including an earlier
ordinal insertion in the helper. A bound stages only after validation and both
primary/secondary deletes. Commit acknowledgment transfers ownership; scope,
future, error or cancellation drops the key and its permit. Generic `first` and
`begin` retain their existing snapshot behavior. No index/identity/configuration,
checkpoint format, SQL syntax or backend execution split changes.

Optional admission uses the existing decoded-value pool: four maximum logical
keys plus headers cover retained/request/replacement copies. The shared internal
admission arithmetic leaves the existing scope, output and point-read headroom;
TTL uses the same unchanged formula. Configured limits do not rise. Pure sliding
or explicit-ID-only batches do not request this optional member progress.

Concurrent shared-pool pressure can change after cursor creation. Only current
chunk **preparation**, including scope admission and event processing, retries
cold on decoded `ResourceExhausted` while holding optional progress. Its failed
scope first releases the key/permit, and optional progress stays disabled for the
rest of that input batch. Acknowledged earlier chunks never replay. Commit stays
outside this retry because application may precede acknowledgment. Other errors
retain their original failure. This reuses the existing cleanup error classifier;
cleanup behavior is unchanged.

## Scan accounting and limits

The occurrence ordinal is unknown, so a point read cannot discover it. Explicit
IDs continue using point reads. Unknown-ID R scans retain the exact encoded tuple
prefix and its existing native exclusive prefix-successor upper bound. The new
logical lower bound is the last deleted key; native Seek receives it inclusively,
and `first_from` filters equality exclusively. One admitted page still performs
one seek. Within a stable chunk snapshot, equality/overlay tombstones can require
additional pages. Configured page/write/overlay bounds are unchanged.

Each chunk creates its own stable snapshot. A logical key, never a native snapshot
cursor, crosses acknowledged chunks. The native iterator exists only for its
admitted page task; page/snapshot reservations retain their original lifetimes.
Input cannot interleave within this serial operator invocation. The next input
batch, error/cancellation and fresh recovery are cold; nothing new is serialized.

The single slot loses reuse when another tuple, group, generation or unknown-index
aggregate intervenes. Input batch 1 also remains cold. This scoped repair does not
claim to remove all historical traversal in those cases or optimize MIN/MAX
output evaluation, dirty output, checkpoint scans or SESSION.

## Finite workload and provenance

Raw receipts, source inventories/copies/patches, immutable executable pins,
commands/exits and logs are outside Cargo caches at
`/home/jason/qa-evidence/str119-duplicates-20261015/`.
All builds and dependent runs use the maintained `scripts/cargo-dev` and
`scripts/rust-build` queue. Profile is normal Rust dev/test with the matching
precompiled RocksDB O0 archive, not optimized production measurements. Both arms
use immutable image
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.

The actual native-member pilot seeds three groups × two values (equal UTF8 and
NULL) ×192 occurrences =1,152 members, with interleaved ordinal gaps. Blocks of
up to 32 equal-tuple deletes alternate groups/values. Exactly 1,104 acknowledged
retractions leave eight per tuple/48 total, checked independently of selected
output through full collection totals and known surviving R entries. Existing
bounded ARRAY output limit 1 permits this retained-state workload without raising
any resource cap. The native ordered arguments and event timestamps stay fixed;
retention is indefinite, so processing-clock samples do not expire members.

A pre-deletion snapshot pins history identically in both arms; ordinary backend
compaction remains enabled. Temporary PerfContext instrumentation runs in each
actual R scan task and logs issued seeks, native child/memtable seeks,
internal/deleted/recent skips, block reads/bytes and native seek/next times.
Process `/proc/self/stat`, `status` and `io` are captured at each completed stage.
Those process counters include setup/background/logging work; native block
counters cover scans, not all point-read/write I/O. Instrumented debug timings
and small retained state do not establish production throughput or RAM capacity.

Comparisons also execute 1,104 exact R-key point gets (mechanism only), 1,104 actual
injected-timestamp explicit-ID member retractions, and 192 actual
`process_native_batch` retractions. The helper stream transfers progress across
one-operation scopes for a finite serial diagnostic; its longer diagnostic
lifetime does not imply reuse across production input batches. It is separately
labeled from actual operator batching. Completed member retractions are the denominator;
source ingestion and sink/CDC output completion are not timed. Matched controls
use round-robin tuple updates (block 1) and actual input batch 1.

## Baseline and preserved failures

`baseline/` preserves diagnostic compilation exit 101: an ambiguous integer in
`is_multiple_of`, repaired with an explicit `usize`. `baseline-retry/` built and
pinned successfully, but diagnostic fixture seeding exited 101 on the existing
ordinary unbounded collection materialization preflight. No production defect or
cap increase was inferred. Source inspection also identified that the old test
helper derived identical IDs for equal rows; the fixture now supplies distinct
opaque IDs through a shared test helper, without changing runtime identity.

Before fixture repair, the independently runnable baseline inventory passed
three store tests and 62 incremental tests (three ignored), both exit 0 on the
same immutable pin. The ignored diagnostic remained blocked by seeding refusal.
The repaired fixed diagnostic selection in `baseline-bounded/` passed build and
pilot, both exit 0. Original failed source/pin/log/receipts remain preserved.

| Completed deletes | Remaining members | Native R seeks | Deleted skips | Internal skips | Diagnostic ops/s |
| --- | --- | --- | --- | --- | --- |
| 192 | 960 | 192 | 2,976 | 3,168 | 1,219 |
| 384 | 768 | 192 | 9,120 | 9,312 | 1,148 |
| 576 | 576 | 192 | 15,264 | 15,456 | 1,040 |
| 768 | 384 | 192 | 21,408 | 21,600 | 851 |
| 960 | 192 | 192 | 27,552 | 27,744 | 749 |
| 1,104 | 48 | 144 | 24,696 | 24,840 | 665 |

Each row is a stage delta, except completed/remaining cardinalities. Total helper
baseline is 1.208225 s, 1,104 seeks, 101,016 deleted skips and 102,120 internal skips.
Actual 192-row operator batching took 0.335723 s with 384 R page seeks and 18,144
deleted skips: the existing three-row stable chunks also revisit pending overlay
tombstones. Actual explicit-ID retractions used zero secondary scans (0.564325 s);
exact-key gets took 0.227305 s. These operations differ in type/accounting work and
are distinct mechanisms, not an interchangeable speed comparison.

The first queued candidate was cancelled before any build began (exit 143) to
repair an independently identified decoded-pressure risk. Its receipt is
`candidate-queued-cancelled.json`. The repair uses the established cold fallback
and the exact failure scenarios described below.

## Matched candidate diagnostics

`candidate-probe-r2/` built/pinned the repaired fixture and passed the pilot,
three store tests and 69 incremental tests (three diagnostic tests ignored in the
ordinary inventory), all exit 0. `pairs/receipt.json` records three alternating
AB/BA/AB pairs and two AB/BA controls: all ten finite arms exit 0. Pins, source
copies and native raw logs remain outside the shared cache. `summary.json`
contains every stage, counter, process sample and comparison.

| Pair | Helper seconds baseline → candidate | Actual batch192 milliseconds baseline → candidate |
| --- | --- | --- |
| AB 0 | 1.055072 → 0.972952 | 285.749 → 255.954 |
| BA 1 | 1.076692 → 0.923054 | 291.192 → 277.491 |
| AB 2 | 1.061309 → 0.994998 | 306.324 → 260.236 |

Each block32 helper arm completes the same 1,104 acknowledged operations and
48 exact survivors. All three baseline arms perform 1,104 seeks, 102,120 internal
and 101,016 deleted skips; all three candidate arms perform 1,104 seeks, 5,052
internal and 3,948 deleted skips. A tuple switch still starts cold, explaining
remaining historical skips. Early/middle/late rates below count completed
384/384/336 operations, respectively, from each arm's six stage deltas.

| Pair | Baseline early / middle / late ops/s | Candidate early / middle / late ops/s |
| --- | --- | --- |
| 0 | 1461 / 1024 / 806 | 1380 / 1126 / 950 |
| 1 | 1320 / 1023 / 819 | 1415 / 1141 / 1066 |
| 2 | 1449 / 1006 / 811 | 1322 / 1078 / 965 |

The **actual operator batch192** arms independently demonstrate scope-boundary
reuse: every baseline arm uses 384 R pages, 1,152 child seeks, 384 memtable seeks,
18,720 internal and 18,144 deleted skips. Every candidate arm uses 320 pages,
960 child seeks, 320 memtable seeks, 511 internal and 63 deleted skips. Existing
three-row chunks retain their stable snapshots; inclusive native seeking of the
last deleted key and overlay tombstones still cause extra pages. This is the
production batching path, separate from the longer-lived helper mechanism.

The round-robin helper controls retain exactly 102,120 internal / 101,016 deleted
skips and 1,104 seeks in both arms. Actual **batch1** controls likewise retain
192 seeks, 18,528 internal and 18,336 deleted skips in both arms. Their timings
are mixed (batch1 baseline 332.332/297.924 ms, candidate 312.904/330.078 ms).
No improvement is claimed for these cold cases. Actual explicit-ID retractions
remain at zero secondary scans in every arm; exact R gets remain separately
labeled mechanism measurements.

Across block32 pairs, helper-stage RSS ranges are baseline 94,404–96,304 KiB and
candidate 94,932–96,220 KiB. CPU deltas from the first completed192 sample to the
completed1,104 sample are baseline 89/88/92 ticks and candidate 80/78/84 ticks
(100 ticks/s). These exclude setup and the first192 operations. Native helper
block reads vary (baseline 717/527/25; candidate 375/23/25), while actual batch192
block reads are baseline 0/0/0 and candidate 0/960/320. Compaction/SST/cache timing
therefore prevents a stable I/O-reduction claim. Final helper process read_bytes
are 73,654,272 for the first baseline arm and zero for the other five; write_bytes
span 1,003,520–1,032,192. Those are cumulative whole-process counters, not isolated
operation I/O. All raw counters and recent skips (zero) are retained. The repeated
finite traversal repair and lower paired debug times do not establish optimized
production throughput, an RSS improvement or universal workload gains.

`candidate-probe/` preserves the first candidate: build/pilot/store exit 0,
incremental inventory exit 101 (67 passed, two failed, three ignored). Both new
fixture failures were inventoried before repair: output-limit24 pessimistic
preflight refused the recovery seed, and equal/NULL helper members reused one
primary ordinal. Recovery now uses 12 ordinary complete-output members, leaving
two of each value before recovery; sparse tuples use disjoint opaque ordinals.
No production change, budget increase or engine behavior change repaired those
fixtures. Initial pressure, maximum-key, exact-budget and cancellation tests all
passed on that failed-inventory pin too.

The temporary state PerfContext probe has been removed exactly: no final state
source diff remains. `probe-to-final-source.json` confirms matching hashes of all
four worker source/test files between the passing diagnostic pin and final
probe-free source. The restored state file SHA256 is
`5dbe209223b0dbeb3bad7db76986aac1456eb6e184329df018319155396ee169`.

## Exact correctness and required checks

Focused tests exercise sparse ordinals/deleted pairs, full tuple-prefix isolation,
NULL and deterministic supported ordering, earlier insertion invalidation,
unmatched-retraction rollback, full collection totals, scoped permit release,
before/after atomic-commit cancellation and fresh retry. Actual native batches
are checkpointed and restored into fresh memory and RocksDB operator/backend
owners (12 seeded → six exact survivors → four after restored retractions).
Cancellation before commit leaves two members; cancellation after application
before acknowledgment leaves one; cold retries produce one and zero, respectively.
The real Rocks dynamic-pressure case acknowledges exactly two three-row deletion
chunks across three snapshots (one cold retry); its non-resource control stops
after one acknowledged chunk and two snapshots. All release the full decoded
pool. Separate tests cover exact original decoded budgets, maximum logical
R keys and real Rocks shared decoded pressure introduced after the first chunk
acknowledgment, with exact snapshot/write counts and a non-resource-error control.

Final probe-free queue job `9bd426116c74` and its foreground runner exited 0.
`candidate-final/receipt.json` records every exact command, elapsed time and
source inventory. Required independent checks completed without repairs:

| Command | Exit / result |
| --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` | 0 |
| `scripts/preflight-clippy.sh -p arroyo-worker` | 0, strict all targets |
| `scripts/cargo-dev fmt --all -- --check` | 0 |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib --no-run --message-format=json` | 0, executable pinned before reservation release |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib arrow::aggregate_store -- --test-threads=1` | 0, three passed |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib arrow::incremental_aggregator -- --test-threads=1` | 0, 69 passed, three ignored diagnostics |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib native_duplicate_churn_pilot -- --ignored --nocapture --test-threads=1` | 0, one passed |

The final no-probe pin SHA256 is
`e4f49559425509f084c81cd0ce0da0b8e405315b921a62df113908cff24d593b`;
the passing diagnostic pin is
`c8559aaf913610d1311647bb133699cbaa62c74b04bb5428c4b829eed32ecbab`.
Both are retained with their source and artifact receipts. All worker source
hashes match between these receipts; only removal of the temporary state probe
separates the builds. Final state sources equal the base, so no changed-state
crate gate is needed. No unrelated failure or repair was observed.
Independent source R2 review approved the current implementation; final source/
evidence review, publication and current-head CI remain with the coordinator.

These tests use real native member/batch methods and fresh checkpoint backends
inside the worker harness. They are not full SQL execution or external worker/
controller process-loss qualification. Broader integrated capacity/1.2M and
process recovery remain their existing consolidated acceptance scope.

## Delivery integration

Rebased reviewed implementation `f53a10ad` onto fetched main
`6791072438b9bbf7c1c32bb1061eebeaaf27687c` (merged build/pin tooling).
Only the ledger required manual resolution; both ticket entries are preserved.
Re-read `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, ledger and
work plan. The new maintained CLI packaging/native selector guidance applies
to future builds; this increment does not create a CLI package. All worker,
state and RPC inputs in the final passing receipt remain byte-identical.
`integration-receipt.json` records that comparison. Local Cargo/Clippy/tests
were not repeated for this documentation-only integration; the exact
probe-free executable evidence above remains source-bound to the unchanged code.
Static whitespace/conflict/scope checks and independent integration review
are delivery gates; current-head CI remains required before handoff.
