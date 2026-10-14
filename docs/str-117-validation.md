# STR-117 SESSION metadata discovery

SESSION insertion now discovers the greatest start at or before the event using
a bounded stable-snapshot backend lookup, then advances through later neighbors
once. It no longer enumerates unrelated earlier intervals or restarts at the
minimum after overlap deletion. Cached Empty/Sole shortcuts remain optional;
ordered successors bypass repeated cardinality proofs and cold minimum reads
reuse their proof page. No SQL, timestamp, arrival-ordinal, checkpoint format,
cache-capacity or configured resource-limit change is introduced.

The user approved [the concrete generic backend contract](str-117-backend-proposal.md)
on 2026-10-15. The new `LastEntryRequest` and snapshot
`last_in_range`/`try_last_in_range` methods return a greatest full encoded entry
within the existing logical range, in a zero/one-entry `ScanPage` with no cursor.
Memory uses bounded BTreeMap `next_back`; RocksDB installs both native bounds
before `SeekToLast`. Oversized greatest entries error rather than falling back
to smaller ones. Routing-hash tie order matches existing encoded ordering.

## Source and established behavior

Worktree `/home/jason/repos/streamr-worktrees/str-117-metadata-20261015`,
branch `jason/str-117-metadata-20261015`, frozen fetched `origin/main` base
`aaab7727200defe3c5b00e13975ef61a379084e1`.
Instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`,
`docs/build-qa-troubleshooting.md`, complete current Trakkt STR-117 and
`/home/jason/.claude/agents/feature-implementation-engineer.md`.
Signing and automatic elapsed-time limits are retired. Maintained container
wrappers and one shared target/reservation are used; no alternate compiler path.

Established intervals are start-ordered and separated by more than the gap.
An event joins at equality with `end + gap`; expiry remains strict watermark
comparison. The retained upstream SESSION implementation's
`session_aggregating_window.rs::fill_active_session` consumes sorted batches
monotonically and `session_prefix` connects equality. Baseline metadata insertion
repeatedly scanned the minimum for both cardinality proofs and successor reads,
and restarted after each removal. Histories remain immutable timestamp/ordinal
rows; the repair changes only discovery, not aggregation or output behavior.

## Scan contract and ownership

| Operation | Why enumeration/order is required | Native bounds and frequency | Progress and ownership |
| --- | --- | --- | --- |
| Greatest predecessor | Its start is unknown; point lookup and forward seek at event-gap omit a long spanning interval | Namespace/group S prefix inclusive lower; exclusive upper is escaped `session_key(event) + 0x00`, intersected with prefix/namespace successors. One lookup for cold/Unknown insert, none for cached Empty/Sole | Zero/one admitted entry; no cursor; complete key/value checked before decode. Page drops before the next awaited read/write |
| Later neighbors | Unknown adjacent intervals may join when end expands | Same namespace/group S prefix; inclusive native start at `max(original examined start,event time)`, caller excludes that row; prefix successor upper. Bounded pages (`min(page_entries,2)`) | One stable snapshot per insertion. Local monotonic start advances after successful delete; page continuation remains snapshot/range bound. Removed entries are skipped in the old view; cancellation discards local progress; retries read fresh live state |
| Empty/Sole proof/minimum | Unknown group cardinality cannot be established by known-key reads | Full bounded S/group prefix; cold minimum lookup only, not each ordered successor | Proof requires no page continuation; first row reused. Proofs invalidate before ambiguous metadata writes; no persisted cache/index |

Rocks last-entry admission accounts for request copies, escaped bound copies,
both owned native bounds, iterator/container and decoded row within the current
scan pool. Await cancellation retains DB view and permits in the uncancellable
blocking closure. Returned `ScanPage` retains its existing permit until drop.
The greatest-predecessor proof excludes any other live start after the selected
predecessor and at or before the event, so the first forward bound skips obsolete
earlier starts without losing
a neighbor. Later original starts retain monotonic progress. The insertion
snapshot releases before final add; a bridge can retain historical
versions during its bounded metadata traversal. Existing memory snapshot/map
ownership remains unchanged. Recovery recreates ephemeral proofs cold; existing
full checkpoint/export/restore machinery is reused. No returned-memory bound
promises constant native work through RocksDB tombstone/version history.

## Finite baseline and verification

Evidence directory: `/home/jason/qa-evidence/str117-metadata-20261015/` outside
Cargo caches. The ignored diagnostic fixture seeds 180 disjoint intervals in
both opaque groups and completes 180 inserts in early/middle/late thirds for
hot-group disjoint growth, bridge-chain merges/deletions, alternating groups and
tight nine-row cache fallback. Every run asserts both groups' exact intervals and
all timestamp/value rows, including duplicate arrival order. Completed inserts
mean awaited state mutation completion; this is not operator-output throughput.

Settings: gap10; keys128B, partial1024B, page8192B/8 entries,
write32768B/16 operations, resident8MiB; shared cache/memtable/write/scan pools
1MiB each, decoded1MiB or tight9216B, blocking2, snapshots8, databases1,
disk reserve0. Both pins use the same immutable Bookworm image and normal
unoptimized dev/test profile with pinned native archive. Initial per-third
CPU/I/O samples allow deltas; RSS is per sample and VmHWM is whole-process lifetime.
Seed/final assertion reads are excluded by explicit per-third phase markers.

Failed/stopped attempts remain separate:

- Original queued-only baseline exited130 at the user's discussion choice before
  acquiring the slot; no compilation or measurement.
- `baseline-pin`: fresh build0, all four workload/value assertions completed, but
  cleanup raced async native shutdown (`DirectoryNotEmpty`), followed by a
  pthread shutdown error; runtime139, not a passing baseline.
- `baseline-retry-pin`: fixture now drops all store/table/observer/snapshot handles
  and awaits native backend close before owned-root removal. Fresh worker artifact
  explicitly `fresh=false`, build0 and diagnostic1/1 passed in99.40s, runtime0.
  Exact source/image/pin hashes and command receipts are in its `receipt.json`.

## Final source and selected gates

All temporary production probes and `STR117_DIAGNOSTIC` environment checks are
removed from final source. Final five-code-file hashes, six gate-log hashes and
base are frozen in `final-gates/source-receipt.json`; `source.patch` binds the
complete reviewed diff. The coordinator independently checked those hashes.
All final commands ran sequentially in the same held maintained build reservation
as final matching and probe removal, with no alternate target or compiler path:

| Command | Exit | Wall including container wrapper |
| --- | --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-state -p arroyo-worker` | 0 | 11.509s |
| `scripts/preflight-clippy.sh -p arroyo-state -p arroyo-worker` | 0 | 17.419s |
| `scripts/cargo-dev fmt --all -- --check` | 0 | 2.918s |
| `scripts/cargo-dev test --locked -p arroyo-state --lib live:: --no-fail-fast -- --test-threads=1` | 0 | 98.055s |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib arrow::session --no-fail-fast -- --test-threads=1` | 0 | 43.629s |

State:71 passed,0 failed,2 unrelated opt-in diagnostics ignored,9 filtered.
Worker SESSION:23 passed,0 failed,1 scoped diagnostic ignored,162 filtered
(execution0.61s). Coverage includes existing actual checkpoint/fresh-memory-backend
closure and cancellation/retry; exact intervals/timestamps/value/duplicate arrival
order in memory and RocksDB under normal/tight decoded budgets; long spanning
predecessor and valid signed-time extrema. Generic last-entry cases include
absent/range/escaped keys, routing ties, oversized/invalid limits, old/fresh
snapshot deletion visibility, admission waiter cancellation, retained page permit
and lower-bounded reverse positioning before unrelated tombstones. This is not a
new Kafka or full production process-loss recovery qualification.

Initial implementation gates and94 selected passes are separately preserved in
`candidate-gates/`; the reviewed timestamp-bound repair passed worker gates and
23 selected tests in `timestamp-gates/` before final matching. No unrelated
failure was repaired. Independent final cleaned-source/evidence review approved;
documentation-only integration onto `cd9520ee89f6ba1e4e9c7362b4b306fc257c78e1` preserves incoming STR-112 records and build guidance. All five validated code hashes remain byte-identical; guidance was reread after the rebase. Publication and current-head CI remain coordinator gates.

## Matched finite metadata work

Final diagnostic ordering is frozen A1 → final B1 → final B2 → frozen A2;
probe-disabled finite insertion runs are A1 → B1 → B2 → A2 within the same held
reservation. Every scoped diagnostic invocation passed1/1, all commands exited0,
and both groups' complete metadata/history oracles and awaited cleanup passed.
Intermediate candidate/pairs are preserved separately; final B uses the reviewed
`max(original start,event)` forward bound, not the intermediate earlier bound.

Pins: A `baseline-retry-pin/receipt.json`, SHA256
`43073e32fcdab949ebc4664ce620e7c3d9638e0dc0c578580fd4a5c6b9522cad`;
B `final-candidate-pin/receipt.json`, SHA256
`d625fca3dc125755807b34fdc9b94b6cff7de9446f51f8afe9448136a06bb59f`.
Both worker artifacts explicitly compiled `fresh=false`; the immutable image is
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Their exact source patches/hashes, profiles, command arrays/exits and runtime logs
remain beside the pins. Raw final pairs are in `final-matched-pairs/`;
`measurement-summary.json` stores every exact third's counters/rates/CPU/I/O/RSS.

Each cell below is early/middle/late,60 completed inserts per third. Both repeats
have identical native-work counters; seed and final assertion reads are excluded.

| Workload | Issued native positioning A → B | Internal-key skips A → B | Deleted-key skips A → B |
| --- | --- | --- | --- |
| Disjoint growth | 25260/32460/39660 → 120/120/120 | 50460/64860/79260 → 60/60/60 | 0/0/0 → 0/0/0 |
| Bridge chain | 360/360/350 → 180/180/179 | 14570/36300/55998 → 4300/11520/18714 | 10980/32580/52382 → 2000/5610/9210 |
| Alternating groups | 7320/21720/36120 → 120/120/120 | 29150/86760/144359 → 180/180/179 | 126/120/120 → 0/0/0 |
| Tight fallback | 7320/21720/36120 → 120/120/120 | 43782/130200/216599 → 180/180/179 | 126/120/120 → 0/0/0 |

Each successful operation here issues one native forward Seek or bounded
SeekToLast. Those counts are not total native child seeks. Memtable-child counts
match issued counts for disjoint/alternating/tight; bridge A694/720/700 versus
B236/240/239. Available `SeekChildSeekCount` was not captured in frozen A and is
not inferred. Internal/deleted counters are reported separately, not added into
a purported count of every physical key visit; returned metadata is separately
recorded in the summary.

Proof/successor scans in disjoint A are12630/16230/19830 **each**, versus B zero
proof scans,60 greatest-predecessor plus60 successor scans per third. Alternating
and tight A proof/successor scans are3660/10860/18060 each; B again60+60. Bridge A
proof180/180/176 and successor180/180/174 become predecessor60/60/60 and
successor120/120/119. Insertion snapshots fall to60 per third in every B arm,
versus disjoint12630/16230/19830, bridge180/180/176 and alternating/tight
3660/10860/18060. These reductions survive the absence of optional caches.

Both source arms retain the same declared intervals/rows: disjoint opaque group
`a` intervals and rows240/300/360 while group `b` remains180; bridge group `a`
intervals120/60/1 with rows240/300/360 and unchanged group `b`180 intervals/rows;
alternating keeps180 intervals per group with210/240/270 rows each; tight keeps
180 intervals per group with group `a`240/300/360 rows and group `b`180. Final
bridge metadata is exactly `[0,3590]`.

## Probe-disabled finite insertion timing and resource samples

These pins still contain opt-in environment-check code, with the probe disabled.
They are unoptimized dev/test **store insertion diagnostics**, not fully
instrumentation-free or optimized production/operator-output binaries. Per-scan
stderr overhead makes enabled-probe timing unsuitable for throughput claims.

Seconds to complete each60-insert third (exact unrounded values/rates in JSON):

| Workload | A1 | B1 | B2 | A2 |
| --- | --- | --- | --- | --- |
| Disjoint growth | 3.683/4.913/6.372 | .051/.048/.052 | .039/.045/.040 | 3.510/4.800/5.829 |
| Bridge chain | .112/.166/.175 | .097/.131/.150 | .096/.123/.157 | .196/.201/.179 |
| Alternating groups | 1.161/3.406/5.907 | .049/.045/.046 | .050/.044/.058 | 1.069/3.202/5.083 |
| Tight fallback | 1.466/4.016/5.836 | .060/.062/.054 | .061/.060/.062 | 1.114/3.014/5.204 |

Bridge improvement is modest and variable; B still has increasing native
internal/deleted traversal as removed starts accumulate, despite bounded
logical lookup. The reviewed event-time forward bound removes a redundant pass;
the necessary reverse predecessor can still cross that history. Do not
extrapolate a constant native-cost guarantee, uniform latency improvement or
production throughput from the stronger disjoint/alternating/tight result.

Process user+system CPU ticks over measured thirds (100ticks/sec), summed per
workload: A1→B1 is disjoint1468→15, bridge43→35, alternating1020→15,
tight1067→16; B2→A2 is13→1439,34→56,15→938,20→946 respectively.
Resolution is one tick; the raw initial/final process samples remain available.
RSS samples range68188–76096KiB for A and68340–75624KiB for B across these runs.
VmHWM is whole-process lifetime across successive scenarios, not a per-case peak.
No configured pool increase or whole-process RSS bound is claimed.

Measured physical read bytes are0 in all four probe-disabled runs; physical
write bytes total12288/0/0/8192 for A1/B1/B2/A2. Process `wchar` totals are
5550367/91661/91668/5551060; these include process/log writes and are not physical
storage throughput. `rchar` totals22721/22567/22566/22634 and all per-third I/O
deltas are retained. This small retained-state workload is not larger-than-RAM
or full1.2M capacity qualification. No full-engine, Kafka architecture or broad
recovery acceptance is substituted by these finite source-bound checks.
