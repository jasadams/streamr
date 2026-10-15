# STR-124 TUMBLE/HOP scan validation

Fetched base `2368459dacd53f9e3aa5024fb5a4f31ba96f8dab`, branch
`jason/str-124-windows-20261015`. Instructions read: `AGENTS.md`,
`.claude/team.md`, `.claude/build-test.md`, `docs/milestone-3-status.md`, its
work plan, `/home/jason/.claude/agents/feature-implementation-engineer.md`, and
full current ticket. Evidence root: `/home/jason/qa-evidence/str124-windows-20261015/`.

## Change and established contract

Measured scans reread the consumed leaf, and one-result lookups requested two
values. The candidate uses the existing lexicographic prefix successor after
complete self-delimiting leaf keys and requests one result for next expiry,
group and partial. Group keys encode length; partial/expiry leaves end in fixed
sequence bytes, so valid keys cannot descend from a consumed leaf. Cursor
validation precedes advancement, including all-ff sequences whose successor
leaves the original prefix. Namespace, time and group bounds still apply.
Retirement uses the same strict leaf bound within its existing write batching.
No native operator production logic, backend production code, caps, cache,
index, admission pool, SQL or checkpoint format changes.

Native windows retain ordinary partial/final DataFusion aggregation. Upstream
[TUMBLE implementation](https://github.com/ArroyoSystems/arroyo/blob/1546c10edbcc5e00f229c7e819480ce5e8acc940/crates/arroyo-worker/src/arrow/tumbling_aggregating_window.rs)
drains populated bins in order; the native store preserves this behavior using
its expiry index. HOP retains overlapping panes and skips empty slide gaps;
terminal MAX drains retained work rather than enumerating every empty tick.

| Path | Native bounds, purpose and frequency | Progress and lifetimes |
| --- | --- | --- |
| Expiry selection | E prefix; no time end because it discovers next populated bin; one-result lookup per relevant TUMBLE/HOP iteration plus terminal proof | Strict leaf successor after consumed E; logical retired key advances after successful writes |
| Group enumeration | G prefix; full enumeration each emitted interval, one group per lookup plus terminal proof | Strict group leaf successor within each interval; overlap intentionally revisits catalogue |
| Pane reads | Exact P+length+group prefix; lower time inclusive, upper exclusive; exact timestamp uses time-prefix successor | Strict full sequence leaf successor, one decoded partial per call; scalar carries first partial; collection preflight deliberately counts/validates before execution |
| Retirement | E prefix; upper E+time exclusive, time-prefix successor for inclusive exact bin; bounded writes remove P/E and update G | Strict consumed E successor within stable snapshot; commit-bound retired cursor across calls; known G timestamp point read preserves future group index |

All four paths discover unknown keys and require enumeration. Known catalogue
point reads remain unchanged and are not claimed optimized. Rocks intersects
namespace/prefix/end as native upper bound and greatest start/prefix/cursor as
seek minimum. One snapshot belongs to a watermark's emission and retirement;
cancellation drops local progress/snapshot and retry sees remaining live state.
No snapshot cursor is serialized; recovery creates fresh ownership. Existing
page/request/write/decoded reservations remain bounded; producer channel holds
one partial under its existing reservation. No resource cap increased.

## Matched evidence and limits

Each build, its pinning and dependent runs use the maintained `scripts/rust-build`
queue and `scripts/cargo-dev`. The expanded matched campaign and final checks share
one held reservation. All use the canonical shared target and immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Dev/test is for correctness/diagnosis; release explicitly supplies optimized
finite throughput evidence. Cargo JSON confirms O3 release and matching
`native=/opt/streamr-rocksdb/release/lib`, static RocksDB archive. Native-source
compilation was unnecessary. Executables/source hashes and exact commands/exits
are frozen outside Cargo caches in each arm's `receipt.json`, `sources.json`,
`source.patch`, driver and process JSON; `release-driver-results.json` records
all eight optimized arm exits0. Timing order is A/B/B/A; native-counter arms are
separate. Timing builds restore the entire original `rocks.rs`, so probes are
absent, not merely disabled. `release-source-restoration.json` proves restoration.

The original pre-edit dev baseline (`baseline/`) passed before production
changes: build0, native0, diagnostic timing0, 3,328 exact partial assertions per
arm. Reviewer-requested expanded fixtures then ran matched baseline/candidate
(`baseline-expanded/`, `candidate/`), all0. The original disabled-probe dev timing
still contains probe construction and is diagnostic only. Its cumulative
CPU/I/O samples include setup/prior scenarios and cannot attribute stage costs.
Expanded/release per-tick and per-watermark samples have before/after deltas.

Direct fixture `str124_scan_stages` uses many128groups×8panes, hot4×128 with
4-way HOP overlap, and16×8 with page1. Reverse arrival and eight retired negative
panes/group create deletion history before measured drain. Retained counts are
1024/512/128; all3,328 read values/order and retirement are asserted. This is
finite preloaded state with deletion history, not concurrent ingestion.
Direct fixture pools: cache/memtable/write/decoded/scan1MiB each, blocking2,
snapshots8, databases1, reserve0; key128/value8192/page32768, entries64or1,
write65536bytes/16ops, state8MiB. No cache/index change.

Actual `NativeWindow` fixture `str124_operator_watermarks` runs scalar/collection
×TUMBLE/HOP with the existing physical-plan builder. It grows accepted partials
19→135 between watermarks, repeats equal0, jumps to1e9, repeats jump, rejects
late20, accepts equality/future input, then actual MAX and repeated MAX. Each
arm completes32 phases and asserts40 exact aggregate outputs (9 TUMBLE and11
HOP per mode), timestamps, collection cardinality/multiset/distinct. Its decoded
pool is4MiB, other original pools unchanged. Declared retained counts are input
arithmetic, not independently measured backend cardinality. Constant ones do
not prove nullable ordering or CDC semantics; separate established tests and
SQL recovery cover their stated scopes. No concurrent insertion during drain
was invented.

### Native traversal

Initial optimized matched counts below; repeats/raw encoded-byte and Next
counts are in `measurement-summary.json` and raw logs. Calls equal direct native
Seek invocations; `child_seeks` is RocksDB SeekChildSeekCount (may0 in memtable),
not direct seeks. `internal`/`deleted` are native PerfContext traversal counters;
returned bytes are encoded page key/value bytes, not heap allocation.

| Scenario/path | Calls A→B | Returned entries A→B | Internal A→B | Deleted A→B |
| --- | ---: | ---: | ---: | ---: |
| many/expiry:E | 8→8 | 16→8 | 1040→1032 | 1024→1024 |
| many/groups-panes:G | 1032→1032 | 2056→1024 | 7240→3616 | 0→0 |
| many/groups-panes:P | 2048→2048 | 2048→1024 | 2048→1024 | 0→0 |
| many/retire:E | 208→208 | 1031→1024 | 2255→2248 | 1024→1024 |
| hot-hop/expiry:E | 131→131 | 262→131 | 390→259 | 128→128 |
| hot-hop/groups-panes:G | 655→655 | 1179→524 | 11790→5240 | 0→0 |
| hot-hop/groups-panes:P | 2572→2572 | 4612→2048 | 4612→2048 | 0→0 |
| hot-hop/retire:E | 131→131 | 639→512 | 767→640 | 128→128 |
| one-entry/expiry:E | 16→9 | 16→9 | 279→265 | 256→256 |
| one-entry/groups-panes:G | 288→153 | 288→144 | 4230→1440 | 0→0 |
| one-entry/groups-panes:P | 512→400 | 512→256 | 624→256 | 0→0 |
| one-entry/retire:E | 136→129 | 135→128 | 518→504 | 256→256 |

Group enumeration and pane result/EOF calls remain necessary at page64. Selection
and retirement retain historical tombstone skips; this patch does not claim to
remove them. Catalogue enumeration repeats for overlap; retirement point reads
are not isolated as an improvement. Native nanoseconds include instrumented
iterator/decode work and do not substitute for clean full-call timing.

### Probe-free optimized timings

Stage milliseconds A/B/B/A below are measured full store calls with identical
fixtures. End-to-end SQL capacity and production RSS are not established.

| Scenario/path | A1 ms | B1 ms | B2 ms | A2 ms |
| --- | ---: | ---: | ---: | ---: |
| many/expiry | 1.349 | 0.940 | 0.837 | 1.514 |
| many/group | 48.343 | 43.793 | 46.637 | 48.659 |
| many/partial | 99.776 | 84.207 | 98.868 | 104.135 |
| many/retire | 80.046 | 68.393 | 81.179 | 82.427 |
| hot-hop/expiry | 6.418 | 6.819 | 6.098 | 7.131 |
| hot-hop/group | 21.285 | 19.367 | 17.594 | 20.533 |
| hot-hop/partial | 83.031 | 82.552 | 72.112 | 78.948 |
| hot-hop/retire | 26.870 | 28.491 | 25.057 | 26.196 |
| one-entry/expiry | 0.864 | 0.555 | 0.440 | 0.872 |
| one-entry/group | 6.555 | 3.862 | 3.535 | 8.784 |
| one-entry/partial | 12.550 | 9.774 | 9.756 | 16.253 |
| one-entry/retire | 12.262 | 10.704 | 11.174 | 15.583 |

| Scenario | A1 rows/s | B1 rows/s | B2 rows/s | A2 rows/s |
| --- | ---: | ---: | ---: | ---: |
| many | 4462 | 5189 | 4501 | 4325 |
| hot-hop | 14883 | 14924 | 16945 | 15421 |
| one-entry | 7943 | 10283 | 10279 | 6170 |

Early/middle/late completed partial rates (rows/s), same arm order:

| Scenario | A1 | B1 | B2 | A2 |
| --- | --- | --- | --- | --- |
| many | 4323/5067/4612 | 5172/5177/5624 | 6702/3894/3889 | 4619/4704/3712 |
| hot-hop | 5401/13783/5992 | 7817/14449/5156 | 4808/19463/7566 | 8876/16900/7593 |
| one-entry | 8740/7940/5190 | 10315/12541/7396 | 14135/10652/6939 | 6450/9243/3861 |

Negative/noisy arms are retained: first hot expiry6.418→6.819ms and retirement
26.870→28.491ms regress; many-group retirement B2 exceeds A1 but is below A2.
All scenarios improve paired total rates, but per-stage universal speedup is
not demonstrated. Late hot rate also regresses in first pair. No extrapolated
production capacity claim follows these bounded optimized measurements.

Actual operator completed-phase totals (40 exact outputs each):

| Case | A1 ms | B1 ms | B2 ms | A2 ms |
| --- | ---: | ---: | ---: | ---: |
| tumble-scalar | 20.979 | 19.087 | 16.103 | 18.706 |
| tumble-collection | 31.600 | 22.159 | 25.626 | 27.975 |
| hop-scalar | 37.427 | 26.937 | 24.711 | 27.369 |
| hop-collection | 54.449 | 44.418 | 42.743 | 49.691 |

Operator totals144.454/112.601/109.184/123.741ms; first candidate TUMBLE scalar
exceeds last baseline, so no universal arm gain. Per-phase CPU deltas total
18/13/13/14 ticks; read0 and write81920 bytes each. These are process-wide
watermark deltas, not operator-thread attribution. Direct per-tick deltas and
all phase details live in summary JSON. Sparse begin/end VmRSS samples are not
peak/stage memory bounds; zero read_bytes does not imply no native reads (cache
and page-cache hits). Setup, insertion and final backend close are excluded
from timed phases. Sink asserts and discards rather than retaining outputs.

## Correctness and recovery

Final probe-free source hashes remained unchanged during worker gates. All
commands below exit0; full argv/environment/image and raw logs are preserved in
`final-gates-worker/results.json` and receipts. Worker pin is copied outside
Cargo caches before dependent tests in the same held reservation.

| Command / pinned harness selection | Result |
| --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` | exit0 |
| `scripts/preflight-clippy.sh -p arroyo-worker` | exit0 |
| `scripts/cargo-dev fmt --all -- --check` | exit0 |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib --no-run --no-fail-fast --message-format=json` | exit0 |
| `arrow::window_native::tests:: --nocapture --test-threads=1` | 11passed,0failed,1ignored |
| `arrow::window_store::tests:: --nocapture --test-threads=1` | 7passed,0failed,1ignored |
| Exact ignored `str124_scan_stages` | 1passed,0failed;3,328 exact partials |
| Exact ignored `str124_operator_watermarks` | 1passed,0failed;32 phases,40 exact outputs |

Both ignored diagnostics were explicitly executed; no ignored selected test is
left unrun. State crate gates are inapplicable: its temporary probe was removed
and production state source has no diff. SQL-test locked build also exits0:
`scripts/cargo-dev test --locked -p arroyo-sql-testing --lib --no-run --no-fail-fast --message-format=json`.
The same reservation pins the SQL executable and runs `python3 /qa/recovery.py`
in the immutable image (exit0). Final combined full source manifests match;
`completion-results.json` records the whole campaign exit0 with no Rust/test
failure. No repair was needed after the complete selected inventory.

| Actual fresh Engine worker checkpoint case | Memory initial/recovered rows | Rocks initial/recovered rows |
| --- | ---: | ---: |
| TUMBLE scalar COUNT/SUM/MIN/MAX | 4/4 | 4/4 |
| HOP scalar COUNT/SUM/MIN/MAX | 7/7 | 7/7 |
| TUMBLE arrays/nulls/distinct | 1/1 | 1/1 |
| HOP arrays/nulls/distinct | 2/2 | 2/2 |

All8 cases pass exact values using controller checkpoints/source batch1. Scalar
cases checkpoint after4 source rows with retained windows; collection cases
after70 of78, proving retained collection state. HOP collection reuses the
existing nullable collection fixture with HOP1s/2s and independently verifies
both overlapping starts/ends, complete78-element multisets and distinct counts.
Raw queries, inputs, captures, initial/recovered JSONL and result statuses are
in `final-gates/recovery/`. Fresh Engine worker tasks are reconstructed using
existing checkpoint machinery within the SQL test process; this is actual
operator recovery, not standalone state simulation and not OS-process loss.

Final worker SHA256 `899277025bdda9f8a46b7c7b7fdb419878b2af63b04eebaca5c03eb7d85bb940`;
SQL SHA256 `528d885a4055703bdd7066028444cd38ddbffa8005da7c0b15c965ab380e0cf9`.
Source/pin receipts: `final-gates-worker/receipt.json`, `final-gates/pin.json`.

The new neighbor test covers memory/fresh Rocks ×pages1/64, empty/ff/ff00 groups,
MIN/0/MAX timestamps, exact reads, consumed/fabricated all-ff sequence cursors,
inclusive MAX retirement and awaited cleanup. Existing tests cover single-entry
pagination, future catalogue survival, exact epochs/recovery, terminal gaps,
nullable paired ordering/filtering, slow output cancellation and refusal/retry
reservation release.

Established native CDC retractions are a known baseline deficiency owned by
[STR-68](https://trakkt.app/issues/STR-68), with settled actual SQL initial/fresh
worker evidence linked in [calendar validation](str-62-calendar-filters.md).
STR-124 preserves append-partial behavior and does not qualify CDC-window
retractions or repair those semantics. This is not a passing retraction check.
Full combined capacity/OS-process-loss qualification remains separate.

## Failure inventory and delivery

Raw `initial-attempts.json`, refusal logs and completion receipts preserve old
image precheck exit2, cancelled unstarted redundant reservations exit130, and
disk-guard exit1 refusals before driver/Cargo. Guard required161061273600 free
bytes while external unrelated Cargo prevented cleanup; no process was killed,
no caps/guard changed and no ad hoc artifacts deleted. Maintained cleanup later
admitted the one held campaign. These are infrastructure refusals, not Rust
failures. All selected current-source gates and recovery passed; no source failure or
repair occurred. No unrelated crate was modified.

Independent final source/evidence review approved the scoped change with no
unresolved findings. Publication and current-head CI remain coordinator-owned
and pending. The implementer made no commit or push.

Delivery integration: rebased onto `11a18a42` (separate updating-join change).
Only the ledger conflicted; both task blocks were preserved. Window source,
backend and build guidance are unchanged from the validated source. This is
not a claim of rerunning executable checks on the delivery commit.
