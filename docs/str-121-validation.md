# STR-121 collection page utilisation

Implementation and finite development diagnostics, 2026-10-15. Frozen baseline
`5862fc2c39c0b3287f3b0e7c69301586a0423c42`, branch
`jason/str-121-collections-20261015`. Evidence is preserved outside Cargo caches at
`/home/jason/qa-evidence/str121-collections-20261015/`.
Current ticket, `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md` and its work plan were read. No automatic elapsed
limit or review signing applies. Extrema and duplicate-update work remains with
STR-120/119. No application contract, public interface, queue, state format or
production resource cap changes.

## Established behavior and change

The existing collection fallback repeatedly called `first_from`, which fetched a
bounded backend page but returned only its first visible member. It repeated the
scan for every selected member and the terminal miss. Existing DataFusion 55
`array_agg` accumulator behavior and the native one-row decode/update path were
traced before changing traversal. Ordered keys, nulls, multiplicity, DISTINCT
and accumulator selection are preserved; unordered SQL output is checked as a
multiset where order is unspecified.

`AggregateScope::visit_prefix` consumes each bounded page while merging pending
insertions, replacements and tombstones in key order. The collection branch uses
it with the existing output-member limit. The noncollection first-member branch
is unchanged in behavior. No collection batch or retained decoded cache is added.
The opt-in finite diagnostic remains a test-local forwarding wrapper, rather
than production instrumentation or a general benchmark framework.

## Scan bounds, necessity and lifetime

Collection member keys are not known in advance, so point reads cannot enumerate
them. Each scan is restricted to the existing member/group/generation/aggregate
prefix and namespace. Existing native prefix lower/upper bounds and exclusive
last-key continuation remain authoritative. Entry requests are the smaller of the
configured page count and remaining output limit; byte limits are unchanged.
Zero limit returns without snapshot or scan. Full enumeration scans once per
page; bounded output stops after its requested occurrences. DISTINCT may produce
fewer output values than the occurrences enumerated.

The scope retains its immutable snapshot. Pending insertions and deletions merge
against that snapshot; later committed mutations are observed by a fresh scope.
No cursor survives cancellation or checkpoint recovery. A completed backend page
and its existing reservation remain alive throughout synchronous member callbacks,
then drop before the next page request. Memory backend pages retain their existing
bounded-request/snapshot ownership (they did not previously carry Rocks scan
permits). Existing single-row Arrow decoding and accumulator/output admission
remain in force; an accumulated output does not borrow the page reservation.
There is no new retention bypass or larger cap. Callback error and cancelled
awaits release scope/page resources. Cancellation is observed between existing
bounded synchronous page work and backend awaits; uncancellable native work keeps
its existing permits until completion.

## Matched finite measurements

All builds used maintained `scripts/cargo-dev` through the shared queue, normal
unoptimized dev/test profile and matching precompiled native RocksDB archive.
Image SHA: `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Disk was checked before large work. Pinned executables, source hashes/patches,
logs and failures are retained outside the cache within the owning reservation.
Candidate pin SHA256:
`5a0a81e70dd63b3625593e10bfa62a74c24f9233144fcc70a54228bceb62f8c9`.

Five scenarios each run three growth phases and three repeats: 45 completed exact
outputs per arm. Each phase retracts and reinserts eight prefix members, including
duplicates/nulls. Ordinary ordered, unordered DISTINCT, bounded7 and zero cases
retain 8/16/24 members with 24-byte text; pages16 entries/65536 bytes,
key256/value16384/output262144 bytes. A separately declared large-value case uses
384-byte text, retained48/96/144, limit1, page128/35840 bytes and valid operating
key512/value32768/output524288 settings. Both arms use identical settings. Common
write/overlay limits524288 bytes, write64 operations; cache/memtable/scan1MiB,
queued/decoded4MiB, blocking2, snapshots2, databases1, disk reserve0.
No production configuration is raised. The large-value first selected value is
NULL; this measures avoided large-member overfetch, not selected long-value Arrow
decode. A separate exact-value test selects a long value.

`matched-pairs/` contains alternating A1/B1/B2/A2 native-instrumented runs and the
same four probe-disabled timing runs: all eight exits0, 360 exact completed output
evaluations, no failures. `measurement-summary.json` retains all phases. Native
issued/returned-row counters match wrapper page/member counts for every repeat.
The following early/middle/late counts are stable across instrumented repeats:

| Scenario | Output members | Pages/seeks A → B | Fetched members A → B | Internal skips A → B | Deleted skips A → B |
| --- | --- | --- | --- | --- | --- |
| ordered | 8/16/24 | 9/17/25 → 1/1/2 | 44/152/280 → 8/16/24 | 80/288/545 → 16/32/50 | 36/136/265 → 8/16/25 |
| unordered DISTINCT | 5/9/13 | 9/17/25 → 1/1/2 | 44/152/280 → 8/16/24 | 77/281/536 → 16/32/49 | 33/129/256 → 8/16/24 |
| bounded7 | 7/7/7 | 7/7/7 → 1/1/1 | 41/97/112 → 7/7/7 | 76/188/225 → 15/15/15 | 35/91/113 → 8/8/8 |
| zero | 0/0/0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| byte limited, K1 | 1/1/1 | 1/1/1 → 1/1/1 | 48/71/71 → 1/1/1 | 56/87/87 → 3/3/3 | 8/16/16 → 2/2/2 |

DISTINCT enumerates8/16/24 occurrences; output cardinality is not backend work.
Native memtable seeks equal issued pages, child seeks0 and block reads/bytes0 in
these small resident fixtures. Skip counters are not total physical traversal.
Backend key/value copy/decode work follows fetched members; per-selected-member
Arrow decode is unchanged. These results do not attribute a codec improvement.

Probe-disabled median completion milliseconds, early/middle/late, six observations
per phase (two arms times three repeats):

| Scenario | Baseline | Candidate |
| --- | --- | --- |
| ordered | 2.162/4.574/7.114 | 0.635/0.846/1.206 |
| unordered DISTINCT | 2.314/4.827/7.605 | 0.565/0.772/1.077 |
| bounded7 | 1.746/2.294/3.033 | 0.573/0.633/0.604 |
| zero | 0.318/0.334/0.296 | 0.278/0.301/0.297 |
| byte limited | 0.910/1.069/0.954 | 0.470/0.450/0.483 |

Completion includes awaited scalar evaluation and array materialization, not
source receipt or durable pipeline throughput. Zero's late phase slightly
regresses. Process CPU deltas are coarse0–2 ticks baseline/0–1 candidate; RSS
97524–101076KiB baseline/96892–101816 candidate, with no demonstrated RSS win.
Per-phase process physical read/write bytes are0; rchar1873–1887 both,
wchar2172–2587 baseline/2164–2216 candidate. These include diagnostic reporting.
This is development diagnostic evidence, not optimized production performance or
large-state/capacity qualification.

## Checks, preserved failures and remaining gates

- `baseline-pin`: build0/run101, missing injected timestamp row ID; fixture repaired
  identically in both arms.
- `baseline-retry-pin`: build0/run101, ordinary full-output resource refusal;
  workload reduced rather than raising its cap. Queued exploratory broader-cap
  attempt cancelled143 before building; no executable or result.
- `baseline-final-pin`: build0/run101, separate long K7 output refusal after four
  ordinary scenarios; long selection narrowed toK1 with caps preserved.
- `baseline-byte-bound1-pin`: build0/run0,45 exact phases.
- `candidate-pin`: build101, all nine Send compiler errors inventoried, no executable.
  Minimal repair adds Send callback bound and replaces an across-await captured
  prefix predicate with explicit prefix checks. Locked worker check then passed0.
- `candidate-send-pin`: build0/run0,45 phases. All eight matched arms and summary0.
- Temporary native probe removed; state source matches HEAD exactly. Format0 and
  diff-check0. Cleanup driver returned1 on overstrict lexical method equality
  after rustfmt closure braces; retained in `finish-owned/`. Hash-verified
  `post-cleanup-audit.json` passes0, normalizing only whitespace and the two
  displayed single-expression closure blocks. Measured production logic is
  unchanged; final edits additionally clarify documentation and assert callback
  reservation lifetime in a test.

Added meaningful checks cover memory/Rocks overlay ordering, limits including0,
errors/retry, cancellation and permit reacquisition, byte-limited multi-page
progress (eight400-byte members at1024-byte pages), unordered multiplicity/null
retractions, selected long values, and multi-page fresh-backend checkpoint restore.
The final integrated serial tests passed as recorded below. Fresh-backend/operator
recovery remains distinct from the actual fresh Engine worker-task SQL recovery.

## Final integrated validation

Coordinator integrated onto `5f326ae5` (merged STR-119), preserving both ledger
blocks and duplicate-update implementation. Independent source/raw-evidence review
approved the integrated change. One shared queue reservation built, pinned and ran
all selected checks with the same normal dev/test profile and native image above.
`integrated-gates/results.json` and `integrated-gates-worker/results.json` record
all exits0 and unchanged tracked build inputs before/after execution.

- `scripts/cargo-dev check --locked -p arroyo-worker`:0.
- `scripts/preflight-clippy.sh -p arroyo-worker`:0 (strict affected gate).
- `scripts/cargo-dev fmt --all -- --check`:0.
- `scripts/cargo-dev test --locked -p arroyo-worker --lib --no-run --no-fail-fast --message-format=json`:0.
  Pinned executable serial filters `arrow::incremental_aggregator::tests::`:
  71passed,0failed,4ignored; `arrow::aggregate_store::tests::`:4passed,0failed.
  Explicit ignored `native_collection_page_utilisation_pilot`:1passed,0failed.
- `scripts/cargo-dev test --locked -p arroyo-sql-testing --lib --no-run --no-fail-fast --message-format=json`:0.
  The existing `collection_recovery.py` driver ran both supported finite cases
  with the pinned SQL test executable:0; maintained exact array CDC comparator
  passed (`integrated-gates/recovery/comparison.json`).

Recovery uses the existing updating-array fixture (12events, checkpoint after8),
controller checkpoints, source batch1, and native aggregates. Memory capture had
initial1/committed2/recovered3 rows; Rocks had initial1/committed4/recovered5 rows.
The maintained comparator validates the respective exact CDC sequences, including
ordered/null/duplicate/filtered/STRUCT values, retractions and deleted groups.
These are actual fresh Engine worker-task checkpoint recoveries, not OS-process
loss, Kafka fault campaigns or full-capacity qualification.

Final pinned SHA256 values (`receipt.json`/`pin.json`):

- worker tests: `74a725f88a1726a0b8fd87ea8eedca64858445a8dd64cb8637f9c31cdb266a62`
- SQL tests: `173f490a617dbf4f8aaeaefabb290a2af303a803036350d1c9413f8cefc6ca88`
- `aggregate_store.rs`: `c158ffa6a2e189c2d2196b9348459eb94f7778371c346646165a407328f93685`
- `incremental_aggregator.rs`: `a2be65db23b16619b551119cd75f4dba887fdc4c748bcd7c088da24a4310df16`

The complete tracked input hashes are preserved in
`integrated-gates/source-manifest.json` and `post-source-manifest.json`;
`source_unchanged` is true. Pinned artifacts remain outside regenerable caches.
Final acceptance review, publication and current-head CI remain coordinator-owned;
full combined qualification remains STR-103/126.
