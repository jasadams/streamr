# STR-118 SESSION history traversal

The scoped repair restricts each history read and retirement to the selected
inclusive timestamp interval, including every arrival ordinal. Row reads begin
strictly after their prior full key. Tight-budget retirement deletes admitted
key/value entries directly through the existing paged deletion path, without
Arrow IPC decoding or row compaction. SQL, event time, window membership,
arrival ordering, checkpoint format and backend/resource limits are unchanged.

## Source and established behavior

Fetched base `277adcb8a2ff8d2821530de4ba4fc2abd613db66`, branch
`jason/str-118-history-20261015`, worktree
`/home/jason/repos/streamr-worktrees/str-118-history-20261015`.
Instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`,
`docs/build-qa-troubleshooting.md`, complete STR-118 ticket and
`/home/jason/.claude/agents/feature-implementation-engineer.md`.
No automatic elapsed limit or review signatures apply.

Upstream retained `session_aggregating_window.rs::session_prefix` and
`fill_active_session` consume timestamp-sorted members monotonically and include
gap equality. Native SESSION keeps each timestamp/arrival-ordinal row immutable
while merging metadata; emission reads every selected member in that order.
Baseline row scans have a timestamp lower bound but the whole group's upper
bound, stop after reading beyond the end, and re-read the previous full key.
The tight decoded-budget retirement path additionally decodes/compacts Arrow
rows even though it only needs their keys. These are implementation costs of
existing behavior, not missing SQL capabilities.

## Native scan contract

| Operation | Why scans are necessary | Bounds/frequency | Progress/lifetime |
| --- | --- | --- | --- |
| Aggregate member read | Selected member keys and count are unknown; all values feed existing aggregate | R/group prefix and timestamp/ordinal interval; one bounded one-entry scan per requested row | One stable emission snapshot, strict logical successor of prior full key; only current decoded Arrow row and admitted page retained |
| Normal retirement | All selected unknown row keys must be deleted | Same bounded interval, min(page_entries,write_operations,write_bytes/max_key) entries per scan, page byte cap unchanged | One stable retirement snapshot; admitted page/write bounds; successful pages may remain deleted on cancellation, retry uses fresh snapshot |
| Tight-budget fallback | Same required key enumeration | Same interval and page byte cap, one entry per deletion page | Existing paged key/value-size validation and write/metadata order; no Arrow IPC arrays or compaction for deletion |

The exclusive upper bound is full `row_key(group,end,u64::MAX)` plus one zero
byte. `ScanRange.end` is exclusive and its bytes are escaped into the native
RocksDB bound before Seek/Next. This includes the largest ordinal at the end
without incrementing signed timestamps or ordinals. Namespace/group bounds still
intersect the logical range. The strict row-read lower bound appends zero to the
previous full logical key. Existing byte escaping preserves order; all valid
subsequent timestamp/ordinal keys compare greater. No new backend scan API is
introduced. Snapshot versions/tombstones may still require internal native work;
finite counters do not establish a constant native-cost guarantee.

## Finite measurement settings

Evidence: `/home/jason/qa-evidence/str118-history-20261015/` outside evictable
Cargo caches. All builds and runs use maintained wrappers and serialized queue,
normal unoptimized dev/test profile, immutable Bookworm image
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`
and matching precompiled RocksDB archive. Initial disk151GiB free.
`settings.md` records all finite dimensions/limits. Each pin preserves source
hashes before/after build/run, exact source patch, immutable executable SHA,
settings, command arrays, exit codes and raw logs.

PAGED/FALLBACK/ONE_ENTRY/BYTE_LIMITED each seed60 intervals in one group with
8 timestamp/ordinal rows each and16 surrounding deleted physical row keys.
HOT seeds3 intervals with180 rows each. A second opaque group retains one row.
Duplicate timestamps span four event times per interval. Each measured phase
completes reads or retirements in three equal thirds; output-completion is
awaited store-operation completion, not full operator/sink throughput. Exact
read-order/value assertions run on reads; retirement phase excludes all emptiness
and remaining-neighbor assertions. After each retirement third every seeded
interval's remaining rows are checked outside phase markers. A focused regression
checks adjacent same-group intervals after individual retirement.

Settings: keys128B partial1024B page8192B/8 entries write32768B/16 operations,
resident8MiB; pools cache/memtable/write/scan1MiB each, decoded1MiB or fallback9216B,
blocking2 snapshots8 databases1 reserve0. ONE_ENTRY uses1 entry and BYTE_LIMITED
uses1400B pages. Explicit admission assertions establish fallback selection;
a nonempty continued page below configured entry count establishes byte limiting.
Native counters observe only R/group history scans inside phase markers.
CPU/I/O are process deltas; RSS is sampled and VmHWM is process-lifetime high water.
Instrumented timing is not production throughput; probe-disabled repeated pairs
remain dev/test diagnostics.

## Measured native work

Baseline `baseline-pin/receipt.json`: worker SHA256
`b8a1fb187c83696736fe5708b516cc9541cf1069538d1039dc8050d23b89fef6`.
Candidate `candidate-pin/receipt.json`: worker SHA256
`6710a8a3afd51613c1550a7b6a93a8df02b018379df32ffb42ad2e0ccaad6141`.
Both artifact receipts explicitly report `fresh=false`; builds exited0.
Initial baseline and candidate diagnostics each passed1/1, exit0. Repeated
instrumented A1→B1→B2→A2 and probe-disabled timing A1→B1→B2→A2 all passed1/1,
all exits0, including every scenario's exact rows/order/neighbors and normal
successful backend close/root removal. Raw pairs are in `matched-pairs/`;
`measurement-summary.json` preserves every phase's complete counters/resources.

Each cell is early/middle/late. Each ordinary third completes20 sessions/160
members; each hot third completes1 session/180 members. Both repeats have
identical native-work counters in all30 phase combinations per arm. One issued
positioning means one native Seek call here; all scans hit memtables, so
`SeekOnMemtableCount` equals issued calls and `SeekChildSeekCount` is zero.
Internal/deleted counters are separate RocksDB metrics, not a purported sum of
all physical visits. Scan-returned entries are separately retained in JSON.

| Scenario / operation | Issued seeks A→B | Internal skips A→B | Deleted skips A→B |
| --- | --- | --- | --- |
| paged / read | 180/180/180 → 180/180/180 | 1000/1000/983 → 160/160/160 | 640/640/624 → 0/0/0 |
| paged / retire | 40/40/39 → 20/20/20 | 1300/1300/1243 → 160/160/160 | 960/960/912 → 0/0/0 |
| fallback / read | 180/180/180 → 180/180/180 | 1000/1000/983 → 160/160/160 | 640/640/624 → 0/0/0 |
| fallback / retire | 180/180/180 → 160/160/160 | 1000/1000/983 → 300/300/300 | 640/640/624 → 0/0/0 |
| one_entry / read | 340/340/339 → 180/180/180 | 1460/1460/1426 → 160/160/160 | 960/960/928 → 0/0/0 |
| one_entry / retire | 180/180/179 → 160/160/160 | 980/980/954 → 300/300/300 | 640/640/616 → 0/0/0 |
| byte_limited / read | 180/180/180 → 180/180/180 | 1000/1000/983 → 160/160/160 | 640/640/624 → 0/0/0 |
| byte_limited / retire | 100/100/99 → 80/80/80 | 920/920/893 → 220/220/220 | 640/640/616 → 0/0/0 |
| hot / read | 181/181/181 → 181/181/181 | 394/394/377 → 180/180/180 | 32/32/16 → 0/0/0 |
| hot / retire | 23/23/23 → 23/23/23 | 222/222/210 → 202/202/202 | 16/16/8 → 0/0/0 |

Row-read returned entries fall from360/360/359 to160/160/160 per ordinary
third for paged/fallback/byte-limited settings; one-entry340/340/339 becomes160
throughout. Hot reads362/362/361 become180 throughout. Only selected members feed
the aggregate in either arm. Normal paged retirement now consumes exactly the
160 selected entries per ordinary third instead of320/320/312 scanned entries;
fallback similarly avoids IPC decoding/compaction of every deleted row.

Read phases retain480 rows/60 intervals in the target group (hot540/3).
After retirement thirds the target retains320/160/0 rows and40/20/0 intervals
(hot360/180/0 rows and2/1/0 intervals); the separate group's one row/interval
survives throughout. Surrounding initial physical deletion history is960 keys
per ordinary scenario and48 for hot, followed by actual selected-row deletions.
Snapshots release between sessions/operations. RocksDB returned pages retain
scan-pool permits. Memory retains its existing snapshot/map ownership and pages
without a scan-pool permit; the one-entry request and byte cap bound fallback
key/value copies, without claiming new memory scan admission. Residual internal traversal on
paged/fallback continuations remains measured; no new index/cache or constant
native-cost guarantee is asserted.

## Timing and process resources

The timing pins contain dormant probe code with its environment flag absent;
they are distinct from final no-probe source. Diagnostic timing is unoptimized
dev/test store completion, not optimized production/operator/sink throughput.
Only selected phase operations count; seeding and retirement oracle scans are
excluded. The read helper includes its exact-value/order oracle work.

Milliseconds to finish each early/middle/late third:

| Scenario / operation | A1 | B1 | B2 | A2 |
| --- | --- | --- | --- | --- |
| paged / read | 45.27/47.02/41.83 | 35.34/36.83/38.26 | 37.37/37.50/33.55 | 38.93/35.57/40.12 |
| paged / retire | 28.89/42.95/38.57 | 21.68/27.13/28.26 | 20.16/23.00/29.38 | 23.21/25.01/28.40 |
| fallback / read | 40.15/41.06/40.54 | 39.21/37.99/38.91 | 36.39/38.28/42.44 | 37.79/32.94/36.08 |
| fallback / retire | 75.63/84.04/92.80 | 49.47/53.79/64.34 | 56.34/58.97/64.22 | 68.93/83.76/78.79 |
| one_entry / read | 58.94/59.25/61.41 | 40.68/42.05/41.34 | 34.61/39.45/41.00 | 55.31/63.25/58.66 |
| one_entry / retire | 59.99/73.38/75.46 | 60.50/69.26/67.01 | 60.60/68.92/60.72 | 53.30/54.70/60.23 |
| byte_limited / read | 42.16/41.39/42.79 | 40.24/35.57/39.80 | 38.94/42.93/42.48 | 43.53/43.26/42.75 |
| byte_limited / retire | 46.85/45.86/50.53 | 34.39/47.70/48.32 | 39.20/44.91/43.52 | 41.83/46.94/47.35 |
| hot / read | 41.37/40.86/41.48 | 39.73/41.62/40.31 | 41.39/41.62/38.70 | 37.53/38.10/33.00 |
| hot / retire | 12.20/12.01/12.06 | 13.35/13.02/12.93 | 12.17/13.47/15.35 | 13.36/11.96/9.95 |

Timing gains are not uniform: one-entry retirement and hot phases have mixed
or slower candidate thirds despite fewer native skips. The repeatable claim is
reduced measured native traversal/returned entries and removal of fallback Arrow
decode/copy work, not a stable overall throughput improvement.

Across measured phases, total user+system CPU ticks (100/sec) are139/118/120/126
for A1/B1/B2/A2. RSS samples range72856–75704KiB for A1,73016–75908KiB for B1,
72696–75736KiB for B2 and72456–75676KiB for A2. VmHWM is whole-process lifetime
across successive scenarios, not a per-case peak. No whole-process memory bound
is inferred from configured backend pools. Physical read/write bytes are0 in all
four timing arms; `rchar` totals56453/56452/56453/56448 and `wchar`
145749/134300/134309/145759 include process/log I/O. Exact per-phase CPU/I/O/RSS
samples and row/s rates remain in JSON; one-tick CPU resolution limits tiny phases.

## Correctness, recovery and failed attempts

The initial complete serial SESSION run exited101:24 passed,1 failed,2 ignored.
Only the new RocksDB checkpoint export fixture failed, requesting4229540 scan
bytes against its1MiB backend pool. Full inventory/log remains `initial-session.*`.
It was not relabeled passing, and all independent diagnostic scenarios were
collected before changing the fixture. Existing checkpoint export reads process
global worker configuration; an unconfigured unit process defaults to1MiB pages,
whose native scan admission exceeds the fixture pool. The existing supported
`checkpoint.rs::native_export_restores_snapshot_during_concurrent_writes` exact
child/configuration pattern supplies bounded checkpoint pages without altering
production code or backend limits. The fixture now runs in a bounded exact child
and initializes explicit worker YAML before configuration access.

Checkpoint worker cache/memtable/write/decoded/scan pools are1MiB each,
blocking2/snapshots8/databases1/reserve0. Its configured page size becomes32768B.
The checkpoint writer's1MiB decoded pool is independent of the tight live reader's
9216B; this check does not claim checkpoint export within a9216B writer pool.
The live backend still uses its original explicit1MiB scan/write pools and
normal1MiB or tight9216B decoded pool. No cap, public API or checkpoint machinery
was changed to repair the fixture. Normal success removes owned roots/config;
a timeout kills/reaps the exact owned child but does not guarantee filesystem
cleanup after failure.

The new recovery matrix covers memory/RocksDB × normal/tight decoded budget ×
one-entry/byte-limited settings. It verifies duplicate timestamps, inclusive
endpoints including ordinal `u64::MAX`, earlier/later same-group neighbors and a
separate opaque group. One actual retirement page commits with one write operation,
then the next admitted retirement write pauses and is cancelled. Real checkpoint
export, old-backend close and restore into a fresh backend preserve the closing
phase and exact remaining rows. Retry retires the remainder and preserves both
neighbors. A separate memory normal/fallback regression rejects malformed keys and
oversized values, keeps session metadata unacknowledged, removes the corrupt test
entry and completes retry without cross-session deletion.

The native planned typed-array/filtered FIRST/LAST fixture now runs memory and
RocksDB under normal/tight decoded pools and asserts exact count, typed arrays,
SQL order, filtered attributes, window start/end and output timestamp. Its partial
limit is8192B, page32768B/1 entry, write65536B/16 operations, decoded1MiB
or tight73728B; tight mode selects fallback retirement without raising limits. Existing
operator slow-collector/cancellation/retry and checkpoint/fresh-memory-backend
output assertions remain part of the selected suite.

Temporary probes have been removed: final `rocks.rs` matches fetched base exactly.
`final-production-equivalence.json` proves measured candidate and final SESSION
production prefixes were byte-identical before the final comment correction.
`final-production-comment-only.patch`/`.json` record the sole later production
change: correcting a Rocks-only admission comment. Non-comment production lines
remain identical; the failed fixture's test setup also changed after measurement. Final no-probe build/Clippy/fmt/test receipts are in
`final-gates/`. All final commands passed in one held maintained reservation:

| Command | Exit | Wall including container wrapper |
| --- | --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` | 0 | 241.35s |
| `scripts/preflight-clippy.sh -p arroyo-worker` | 0 | 53.58s |
| `scripts/cargo-dev fmt --all -- --check` | 0 | 3.29s |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib --message-format=json arrow::session --no-fail-fast -- --test-threads=1` | 0 | 162.92s |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib arrow::session_store::tests::str118_history_comparison -- --exact --ignored --nocapture --test-threads=1` | 0 | 6.68s |

The complete serial SESSION suite passed25,failed0,ignored2,filtered175
(execution2.41s); the unrelated STR-117 diagnostic stays opt-in and unrun.
The selected STR-118 diagnostic passed1/1 on final no-probe source. The recovery
exact child passed all eight backend/budget/page combinations (1 outer test),
including normal successful root/config cleanup. Native typed output passed all
four backend/budget combinations (1 outer test). These are not eight/four extra
unit-test counts. State gates were not repeated because final state source is
unchanged.

`final-source-touch.json` lists every worker/state Rust file touched before the
final build to prevent shared `/app` target reuse across worktree switches and
proves all content hashes unchanged. Final source hashes match before/after all
gates. `final-gates/final-pin.json` contains the actual newly compiled worker
artifact with `fresh=false`, immutable image, source/log hashes and preserved
no-probe executable SHA256 `092f4413f519c284b1c0fd3c51e5684cc91b761f2021205fad10634b88855f7f`. The copy remains outside Cargo caches and
was made before releasing that same reservation.

Full larger-than-RAM/1.2M combined capacity and process/broker recovery remain
separate qualification. Store/backend restore and native unit operator checks
do not substitute for full process-loss/Kafka recovery or production throughput.
Publication, current-head CI and final independent written verdict are coordinator
gates, not asserted here.
