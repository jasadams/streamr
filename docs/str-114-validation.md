# STR-114 shared RocksDB traversal

Worktree base: `fa9c8133d3feefb4343e322c5405e843e25e1b61`. Instructions:
`AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, and the
feature implementation role. Current user instructions remove historical
automatic time limits and review signing. STR-112 owns checkpoint/export changes;
this ticket does not redesign that protocol.

## Scope and decision

The merged STR-109 native upper bounds and STR-113 SESSION logical minimum are
preserved. Shared scans use one native iterator/seek per admitted page, decode
only returned entries, and return a cursor tied to the exact snapshot/range.
A source observation is not a measured speedup. The finite measurements below justify a shared-backend no-change conclusion:
no new redundant native seek or per-row traversal defect was demonstrated.
Production backend code is unchanged. The isolated repetition cost comes from
caller-selected fresh minimum restarts through same-prefix deletion history.
Caller investigations remain with their existing STR-115+ owners; this ticket
does not create new deferral tickets or imply those paths are efficient.
Cursor re-encoding once per page remains an unmeasured candidate, not a repair.

Every new/changed RocksDB scan now requires explicit reporting in `AGENTS.md`.
The earlier scan audit was source-only; its local original is
`/home/jason/qa-evidence/str113-session-drain-20261014/rocksdb-scan-audit.md`.
Current reconciliation additionally includes aggregate last-row retraction
member/index emptiness proofs, cleanup later-generation marker existence, and
native SST file-metadata sampling.

## Reconciled production caller inventory

Paths below are relative to `crates/arroyo-worker/src/arrow/` unless a crate is named. The earlier audit used PR46 `f8893e77`; independent reconciliation on this fetched base found no changed caller contract and the additional subpaths below. Historical line references identify operations, not a claim that old source measurements apply to this base.

| Production path | Why a scan is used instead of a point read | Bounds, progress and assessment |
| --- | --- | --- |
| SESSION deadline minimum, `session_store.rs:685`, operator `session_native.rs:527` | Find earliest due session when its key is unknown. | Deadline prefix and watermark upper bound; one result per closure. PR46 retains an inclusive admitted logical minimum, invalidated before insertion and cold after restore. Cost measured in STR113. If this optional cache cannot be admitted, the correct but slower prefix-restart fallback remains. |
| SESSION global deadline peek, `session_store.rs:717–719` | Learn next deadline or global empty proof after the watermark-limited range is empty. | Same snapshot and logical minimum, deadline prefix with watermark end removed; at most one extra scan per uncached/invalidated empty check. Independent cost unmeasured. |
| SESSION metadata discovery, `session_store.rs:399–480` | Find sessions belonging to one group, including neighboring intervals needed for merges; prove empty/sole metadata. | Group prefix. Advancing lookup honors `after`, but on cache miss the preceding empty/sole proof scan always restarts at prefix beginning. **Unmeasured candidate:** repeated proof scans during many disjoint sessions, merges/deletions, alternating groups or constrained cache. |
| SESSION row reads and retirement, `session_store.rs:802,915` | Enumerate all retained rows for a session to aggregate or delete them; row keys are not all known. | R/group prefix, logical start at session start, advancing logical keys or stable-snapshot continuation. No normal per-row/page minimum reset. Native end does not encode the session endpoint: it is checked after reading, so traversing deletion history beyond that endpoint within the group is an unmeasured bound candidate. A cancellation retry restarts at session start. STR113 measures aggregate/read and retirement stages together. |
| TUMBLE/HOP expiry, group and pane enumeration, `window_store.rs:552,691,725,780`; `window_native.rs:810–894` | Locate populated time bins, discover their groups/panes, then retire expired rows. | Time/group prefixes and range endpoints. Stable snapshot; retirement carries `retired_through`, group and pane loops advance last keys/cursors. No identical restart loop found. Enumeration cost is justified, performance not measured by this audit. The naive `expire_page` and `earliest_time` helpers are test-only. |
| Aggregate TTL expiry, `incremental_aggregator.rs:1523–1605`, caller `:1754` | Discover groups whose ordered expiry entries are due; keys unknown. | E prefix; stops at future deadline. Advances within a chunk, but `after=None` is reset on every new chunk while the caller drains chunks and deletes expiry entries. **Unmeasured candidate:** repeated tombstone traversal across chunks. |
| Aggregate cleanup queue and member deletion, `incremental_aggregator.rs:1608–1742`, caller `:1761` | Find pending cleanup markers and enumerate obsolete generation members/indexes to reclaim them. | C prefix minimum restarted each call; phase-specific member prefix progresses within a bounded chunk but starts over next invocation. **Unmeasured candidate:** both marker and member-prefix deleted-history revisits. Page admission bounds returned work, not necessarily internal RocksDB traversal. |
| Aggregate duplicate-value retractions, `incremental_aggregator.rs:1018` | Find an occurrence ID for a value when no explicit row ID is supplied. | Exact secondary value prefix; minimum lookup per retraction. **Unmeasured candidate:** repeated deletes of equal values can restart over earlier deleted occurrence IDs. Point read already used when explicit timestamp/ID provides the key. |
| Aggregate MIN/MAX and collection evaluation, `incremental_aggregator.rs:1082–1137` | Ordered extremum key unknown, or collection semantics require enumerating selected members. | Exact group/generation/aggregate prefix. `after` advances collection traversal; MIN/MAX fetches first member. **Unmeasured churn risk:** repeated extremum retractions can leave deleted entries before the current minimum. A full collection evaluation also intentionally revisits live members. Neither is a proven current bottleneck. |
| Dirty aggregate output, `incremental_aggregator.rs:1770–1950` | Enumerate changed groups to emit updates, rather than every group. | D prefix; last key advances across chunks within a drain invocation. New flush starts afresh. No identical per-row deletion restart within one drain; repeated flushes can still encounter historical deletions. No measured defect here. |
| Calendar pruning and evaluation, `calendar_native.rs:369,634` | Enumerate obsolete buckets for deletion, or merge the buckets inside a requested calendar horizon. | Pruning stores its last key/progress; evaluation starts at horizon lower bound and advances until last bucket. No identical per-row restart found; range cost remains unmeasured here. |
| Updating join fanout, `updating_join.rs:415–442` | Enumerate all matching rows for a join key; outer join also needs existence/sole-match checks. | Exact opposite-side join-key prefix; main loop advances `after`, probe-row limit enforced. **Unmeasured repeated-work candidate:** outer-join right-side minimum/existence checks are repeated inside the opposite-row loop; also subject to same-prefix churn. Some fanout is required by SQL output cardinality; repeated existence work needs separate justification. |
| Checkpoint export, `arroyo-state/src/live/checkpoint.rs:339–359,484–485` | Serialize every owned state entry; point reads cannot discover unknown keys. | Whole namespace enumeration once per checkpoint, one stable snapshot, continuation cursor advanced. No delete/restart loop. Full scan cost scales with retained state; not optimized or measured here. |
| Restore empty-target check, `checkpoint.rs:971–1003` | Check absence of any owned entry when its key is unknown. | One-result namespace scan once per restore check. No repeated delete loop. Tombstone-heavy reused storage could cost work; expected target is a fresh restore attempt. |
| Public typed-table enumeration, `typed_table.rs:413–452` | API explicitly requests ordered/paginated enumeration. | Stable table snapshot; caller cursor forwarded and continuation returned. No production caller found under `crates`; tests exercise it. External callers must advance continuation. |

All worker paths above use `crates/arroyo-worker/src/arrow/` unless another crate is named. The shared aggregate range implementation is `aggregate_store.rs:259–330`; its callers determine whether logical progress survives repeated calls. The shared Rocks backend now enforces namespace/prefix/end bounds and continuation seek position for these requests. The standalone live-state probe example advances its continuation; other direct native iterators found under `crates` are tests.


Additional production subpaths confirmed on the fetched base:

| Production path | Why order/enumeration is required | Bounds, frequency and mutation/recovery progress |
| --- | --- | --- |
| Aggregate last-row retraction proofs, `incremental_aggregator.rs:1384–1385` | Prove both primary members and occurrence indexes empty; unknown member IDs prevent point reads. | Exact M and R aggregate/group/generation prefixes, first result, once per batch aggregate after final row retracts. Shared overlay observes pending deletes; repeated cold proofs may revisit same-prefix history. No persisted cursor, fresh scope after recovery. Unmeasured caller-specific churn. |
| Cleanup T-phase later-generation marker check, `incremental_aggregator.rs:1643` | Find a later cleanup generation when its number is unknown. | Exact group C prefix with exclusive logical `after` current marker, first result per T-phase cleanup invocation. New scope after recovery; later generation insertion is visible on next scope. Independent cost unmeasured. |
| Native file-metadata sampling, `live/health.rs:46–55`, `live/rocks.rs:151,275` | `total-sst-files-size` sums owned SST file metadata rather than key/value state. | At open and at most once per second during admitted writes. This property has no keyspace iterator bounds/cursor and does not retain a read snapshot or decode values; native file count determines work. Separate from keyspace scans; no measured change here. |

### Shared range and lifetime contract

Every RocksSnapshot keyspace scan validates the snapshot/range cursor before
admission. Native Seek receives the maximum of encoded namespace/start/prefix
and full encoded cursor; logical starts are inclusive, cursor continuation is
strictly after the full encoded key (including routed hash). Native upper bound
is the minimum of namespace successor, prefix successor and exclusive end.
Byte escaping and the distinction between logical and full encoded keys remain
unchanged. Rust checks remain in place; native bounds act before Seek/Next.

Each page obtains the configured scan reservation before the blocking operation;
the decoded page retains it through consumption or cancellation/drop. Request
and decoded/cursor copies fit the existing admission formula. The iterator
exists only inside one blocking scan task; the owned native snapshot lives for
its ReadSnapshot and any in-flight task. Snapshot admission remains held until
all such references drop. A page does not advance or replace that snapshot.
Point reads/multiget use their separate configured decoded limits. All wrappers
share this backend rather than implementing an alternative SQL executor.

Aggregate scan scopes combine the stable native view with admitted pending-write
overlays; callers' logical `after` is distinct from a native snapshot cursor.
Stable continuation sees the same read sequence despite concurrent deletes or
inserts; fresh snapshots see committed changes and can encounter deleted history
again. Recovery creates fresh views/cursors, never resurrects a stale snapshot
ID; SESSION optional admitted minimum is cold after restore and invalidated
before insertion. No new persistent index or snapshot contract is introduced.

Frequency follows each caller above: paged drains run once per admitted chunk,
minimum/existence calls once per decision, member scans per requested collection
or join fanout, and export once per checkpoint. Necessary output enumeration
does not excuse a restarting minimum or redundant existence probe. Measurements
below characterize the shared mechanisms; caller-specific unmeasured costs
remain with their owner tickets, including checkpoint/export STR-112.

## Predeclared finite measurements

The exact pre-execution plan is preserved at
`/home/jason/qa-evidence/str114-scans-20261014/workload-plan.md`. Native diagnostic:
600 same-prefix and 600 unrelated-family entries, short/long byte-escaped keys
(16/96 bytes), values 32/1024 bytes, pages 1/16, three alternating repeats.
Compare fresh minimum/redelete, stable snapshot continuation/delete, and fresh
logical progression/delete. Pin pre-deletion versions and disable compactions
only in the native diagnostic. Report completed thirds (200 rows), issued/native
seeks, internal/deleted-key skips, reads and native traversal timings.

Actual adapter diagnostic uses existing worker Checkpoint durability (no local WAL/fsync); completion includes scan, assertions and bounded deletes. Same finite dimensions, three repeats, one blocking
operation/database slot, two snapshot slots, 128 KiB scan/write pools, 64 KiB
decoded pool, 1 MiB block cache/memtable. Seed writes use eight-row chunks to fit actual encoded-size × 5 admission. Two configurations couple dimensions; this is not an independent factorial matrix. Stable admitted cursor drain deletes
each completed page and checks exact decoded keys/values and fresh-view final
empty state. Report completed thirds, CPU-tick and OS-I/O segment deltas, and current process RSS samples (not proved peak RSS).
The page-16 arms end each 200-row segment with an eight-row partial page.
Native thread-local PerfContext counters are gathered in the raw traversal
diagnostic, separately from async adapter timings. Neither is SQL throughput,
application output completion, larger-than-RAM qualification, or full STR-103
qualification.

## Validation

Selected baseline gates ran sequentially in the required container. Locked
`arroyo-state` check, strict all-target Clippy and formatting passed (exit 0).
The initial `live::` runner returned exit 101 (66 passed, one failed), but
its executable was stale: `blocking_parquet_export_restores_native_snapshot_during_concurrent_writes`
is absent from this fetched source and belongs to the STR-112 worktree. Its
4,194,916-versus-2,097,152 admission failure is preserved in `baseline-gates/3.log`
as an invalid-source run, not a demonstrated baseline product failure. Shared
`/app` paths and newer target artifacts can mask older worktree input timestamps.
No test/cap repair is made here.

The current-source diagnostic reservation refreshed state crate input timestamps
without changing contents or clearing the shared target, required Cargo JSON
`fresh:false` for the state test executable, and pinned source hashes/binary/image
before runtime. The exact compiled list includes both unique probes and excludes the foreign
STR-112 test. Strict all-target Clippy and formatting passed (exit 0). The pinned
current-source `live::` suite passed: 65 passed, zero failed, two opt-in probes
ignored, nine unrelated tests filtered. Each ignored probe then ran separately:
one passed, zero failed, exit 0. These tests include native bounds/escaping/routed
keys, snapshot stability, page reservation/cancellation, worker durability,
backend/reopen conformance and checkpoint/restore correctness. No omitted
SQL/process-loss/capacity check is claimed passed.
Evidence lives outside evictable Cargo caches at
`/home/jason/qa-evidence/str114-scans-20261014/`.

## Completed measurements and decision

Native diagnostic totals below are identical counters across all three
alternating repeats. Each arm completes exactly 600 checked rows/deletions.
Initial state contains 600 B-family live rows and 600 D-family rows; D rows are
deleted before drain. A pre-deletion snapshot retains the original versions.
At reporting boundaries B live cardinality is 400, 200, then zero, with 200,
400, then 600 same-prefix deletions. This deliberately exposes retained history.

| Coupled configuration | Read/progress choice | Issued seeks | Internal-key skips | Deleted-entry skips | Median full drain (ms) | Median early/middle/late completed rows/s |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| key16/value32/page1 | Fresh minimum restart | 600 | 180,300 | 179,700 | 296.412 | 4,517 / 2,000 / 1,306 |
| key16/value32/page1 | Stable snapshot cursor | 600 | 1,199 | 0 | 61.399 | 9,731 / 10,167 / 10,195 |
| key16/value32/page1 | Fresh advancing lower key | 600 | 1,199 | 599 | 56.435 | 10,850 / 10,906 / 10,377 |
| key96/value1024/page16 | Fresh minimum restart | 39 | 12,144 | 11,544 | 39.113 | 29,546 / 15,545 / 10,157 |
| key96/value1024/page16 | Stable snapshot cursor | 39 | 638 | 0 | 12.687 | 48,650 / 42,132 / 52,233 |
| key96/value1024/page16 | Fresh advancing lower key | 39 | 638 | 38 | 11.790 | 48,972 / 49,838 / 52,840 |

Native seeks decompose into three child seeks and one memtable seek per issued
seek in these fixtures (1,800/600 or 117/39 per drain), not extra adapter seeks.
Recent-version skips are zero. Block reads/bytes are equal across modes within
each configuration: 11/44,541 and 151/739,422. The extra fresh-minimum cost is
internal traversal, not increased read bytes. For page1, deleted skips increase
19,900 → 59,900 → 99,900 across completed thirds; page16 increases
1,248 → 3,848 → 6,448. Native seek/next timing counters are retained in the raw
log. The existing native upper-bound regression also passes and proves an
empty B range skips zero unrelated D tombstones versus 1,000 without the bound.

These read choices have different visibility/progress contracts. The ratios
above are mechanism diagnostics, **not a proposed automatic substitution, a
before/after production optimization, or production throughput**. Stable
continuation is already supported; generic fresh minimum calls cannot silently
skip later inserted lower keys or reuse another snapshot's cursor. No shared
cache/index/API or checkpoint change is justified by these results.

Actual admitted adapter results use worker Checkpoint durability and the exact
fixed pools declared above. Seed chunks are eight rows. Its stable snapshot
retains 600 initial rows while fresh committed cardinality falls to zero; every
decoded key/value and final fresh-view empty state is asserted.

| Coupled configuration | Full drain repeats (ms) | Median early/middle/late completion rows/s | Median early/middle/late admitted scan time (ms) | Sampled process RSS (KiB) |
| --- | --- | --- | --- | --- |
| key16/value32/page1 | 136.292 / 133.348 / 131.899 | 4,171 / 4,676 / 4,655 | 22.420 / 19.583 / 19.898 (200 pages each) | 36,140–38,580 |
| key96/value1024/page16 | 59.962 / 57.322 / 59.872 | 10,904 / 10,341 / 9,644 | 6.768 / 7.737 / 8.331 (13 pages each) | 37,512–38,684 |

CPU deltas total 40 ticks for all three short configurations and 19 ticks for
the long configurations; `/proc` ticks are coarse process-wide samples, not
precise per-scan CPU attribution. OS read bytes are zero (warm page cache);
short-config OS writes are zero and long-config writes total 86,016 bytes
(28,672 in each first segment). Current RSS samples span 35.29–37.78 MiB; this
is not a proven high-water mark or backend-only memory. Raw native block reads
are different from OS disk reads. Completion includes assertions/deletes;
separately timed scans/writes remain in `adapter-diagnostic.log`. Longer-key
scan time rises across thirds in this small fixture; the result is not a claim
of universal constant latency or absence of caller-specific bottlenecks.

Both diagnostics use the unoptimized Rust dev/test profile and pinned RocksDB
10.4.2 O0 native archive; timings cannot establish optimized production rates.
The native diagnostic deliberately uses default native DB resources, disabled
compactions and raw unencoded values. The admitted adapter uses normal worker
options, encoding and its bounded pool. Their absolute times are not directly
comparable. No diagnostic injects production hot-path counters. These finite
600-row workloads do not qualify state larger than RAM, SQL output rates, full
recovery/process loss, all store owners, or the full 1.2M/STR-103 campaign.

## Source, pins and reproduction

One queue reservation formatted/linted source, compiled, pinned and ran all
selected current-source tests. Cargo's state test artifact receipt records
`fresh:false`; the executable cannot be mistaken for the initial foreign
artifact. Image: `e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Test binary SHA-256:
`5cdda9b7c172615a490fcc281123dfc081f3a06298769b8a8e71ab24f3783210`.
The production prefix of `rocks.rs` is byte-identical to the fetched base; only
its cfg(test) module gains the two opt-in finite diagnostics.

The complete tested queue driver is
`/home/jason/qa-evidence/str114-scans-20261014/run-diagnostics.py` (also archived
in `diagnostics/`). Its timestamp refresh is a content-preserving `touch` of
`crates/arroyo-state/**/*.rs` and its `Cargo.toml`, performed inside the owned
reservation before compilation. Run it with `scripts/rust-build python3
/home/jason/qa-evidence/str114-scans-20261014/run-diagnostics.py`.

Reproduce the finite test individually with required container wrapper: `scripts/cargo-dev test --locked
-p arroyo-state --lib live::rocks::tests::native_scan_churn_diagnostic --
--exact --ignored --nocapture --test-threads=1`, and replace the test name with
`admitted_scan_churn_diagnostic` for the actual adapter. Refresh current crate
input timestamps and build/pin under one shared queue reservation when switching
worktrees; verify unique probe names before runtime. The exact executed driver,
commands/exits/durations, source patch/input hashes, Cargo artifact JSON, complete
test list, binary, image and raw/summary logs are under the evidence root's
`diagnostics/`. Invocation wall receipts exclude queue wait; segment measurements
exclude compilation and container startup. Independent source review found no
material findings; final evidence review/publication/current-head CI remain the
coordinator's delivery gates. No merge/release/full milestone pass is claimed.
