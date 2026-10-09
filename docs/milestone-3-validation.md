# Milestone 3 implementation and verification

This branch builds on milestone 2 PR #4 at `fbd5179a`. It carries the existing
capture harness originating in PR #5 and adds the first STR-16/29 implementation slice:
shared execution accounting, durable dual-clock timers and paginated ranked
collections. These are generic engine capabilities. Application schemas and
business behavior remain in the consuming application. See
[the support matrix](milestone-3-support-matrix.md) for retained paths and
interface gaps requiring discussion before further implementation.

The revised milestone plan is recorded in the support matrix: native aggregates
(STR-17), TUMBLE/HOP (STR-19), SESSION (STR-20), typed result composition
(STR-29), state tables/MERGE (STR-38–42) and obsolete SQL surface cleanup (STR-43).
This revision does not qualify those paths or change the historical evidence
below. Application query proposals and oracles remain in the application repo;
they are not embedded engine implementations.

## Current handoff — 2026-10-08

Milestone 3 remains incomplete. This section supersedes older statements about
queued v14b runs; the historical evidence below retains its original revision
and scope. The current candidate is an uncommitted batch atop
`b6b2aa7a3e0fd479f7ea5693ffd9b4d94cc2d95b`; published PR checks do not verify it.

### Review repairs after the frozen v14b run

Fresh independent review found two defects in the unpublished batch:

- Public SESSION and unordered packaged-capture comparators retained full
  datasets in supervisor memory. They now use incremental parsing and an exact
  SQLite-backed multiset through `scripts/bounded_row_oracle.py`. Eight pure
  Python tests passed and passed again under independent review; receipts and
  reviews are in `target/milestone-3-pr6-publication/`. Memory scales with the
  largest working row plus fixed parser/cache storage, not dataset cardinality.
  This is not a fixed process-RSS guarantee or a new SQL/recovery qualification.
- Startup network failures awaited a bounded control channel whose consumer
  starts after link setup completes. Saturation could prevent startup from ever
  finishing. The repair registers deferred awaited reporting with the existing
  network-task owner. New tests cover full-channel delivery and cancellation;
  runtime reporting remains unchanged. Both startup regressions passed in the
  fresh v15 library gate:
  `network_setup_failure_does_not_wait_for_startup_control_consumer` and
  `network_setup_failure_report_is_cancelled_with_engine_network_tasks`.

Only `network_manager.rs` differs from the frozen v14b compiled-source inventory;
**the v14b gates do not verify this repaired Rust source**. The original run
artifacts remain unchanged. Fresh v15 validation uses the required Bookworm
container and shared target. All five gates exited zero: formatting, workspace
all-target check, strict Clippy, library tests and workspace all-target build.
Library logs record 25 suites, 708 passed, zero failed and six ignored; the library
gate took 1308.74 seconds. Exact commands, exits and logs are retained in
`target/native-m3-reviewed-repairs-v15-build-evidence/results.json`, with frozen
source in the adjacent `source.json`. Root session `4624` is terminal with exit
zero; the all-target build took 229.81 seconds.

The 12 small public-helper integration cases passed: hot8 SESSION, many4
SESSION and packaged unordered operations, each across memory/RocksDB and
controller/leader. Evidence is retained in
`target/native-m3-reviewed-repairs-v15-runtime-smokes/integration-evidence.json`
(SHA-256 `46a00cb7a53c8d954017eed2a27b7810915ade2a3bafeabb307425fb1a4cc3b1`)
and the adjacent `results.json`, capture logs, outputs and measurements.
Independent retained-output comparison verified all complete initial/recovered
typed rows and exact multiset multiplicities: one hot session, four many-key
sessions and 161 operations rows per case. SESSION checkpoint input prefixes
are seven rows with zero committed outputs; operations prefixes contain 80
exact committed rows. All eight SESSION measurements verify checkpoint-prefix
values. Peak SQL-child RSS across the 12 cases was 184,414,208–195,432,448 bytes,
below each 512 MiB bound. SESSION retained-payload floors are only 448 bytes;
these small cases do not establish capacity, full lifecycle parity, production
process loss or the 24-hour gate. Large SESSION fixtures remain prepared only;
actual large SESSION retained-payload qualification has not run.

Two workflow interruptions preceded these results, without starting any smoke
case: scheduled `cargo-sweep-all` removed the SQL-test executable between queue
reservations, then the successful 1190.44-second rebuild's driver assumed the
old Cargo artifact filename. The coordinator pinned the actual fresh all-target
SQL artifact `arroyo_sql_testing-4c0060e4a785ebe5`, and continuation session
`66569` exited zero after all 12 cases. Original five-gate receipts remain
unchanged. The rebuilt SQL test fingerprint declares `default` plus
`integration-tests`; its retained listing has 60 tests. This is distinct from
the original library-gate ELF, whose SHA-256 is
`a769438d85f91253fb96745f5e958f360f75f998e39baa7b9f904ef864a1f7f0`.
The smoke SQL executable is pinned as `bin/sql-test`, SHA-256
`ee64553b221e2caefa7a132ef9323f56de82347740fcd61126b2b3bc913b0244`;
the rebuilt production executable is pinned as `bin/arroyo`, SHA-256
`f619e8d6c300364f7667fa48311d80b14f90a7a2a7eba2346257a2627a060bf2`.
Rebuild receipts, fingerprint/depfile metadata, helper/source hashes and packaged
helper/binary inventory links were independently verified against retained files.
Startup regressions belong to the original library gate; these rebuilt SQL
captures do not replace that gate. No source or helper bytes changed.

Container access and Git index writes work. The repaired batch remains
uncommitted and unpublished atop `b6b2aa7a`; PR #6 remains draft and current
candidate CI is pending publication. Preserve the patch and source/hash receipts;
verify remote head and CI against the eventual published commit separately.

The frozen v14b build evidence lives in
`target/native-m3-admission-v14b-build-evidence/`. Formatting, workspace
all-target check, strict Clippy, library tests and full all-target build passed.
The library logs record 25 suites, 706 passed, zero failed and six ignored.
`build-evidence.json` binds the compiled sources and executable hashes; preserve
these files and use their copies of logs rather than relying on temporary paths.

| Completed run | Saved evidence | Scope and limitation |
| --- | --- | --- |
| State-table parity: 40 cases passed | `target/native-str43-parity-admission-v14b/` | Five generic fixtures, batches 1/8, memory/RocksDB and controller/leader; finite typed values and fresh-worker recovery, not full application parity. |
| MERGE export faults: four cases passed | `target/native-str43-merge-fault-admission-v14b/` | Failed export nonpublication, selected epochs, retention and exact recovery; not production process loss. |
| Production process recovery: six cases passed | `target/native-process-recovery-admission-v14b/matrix-results.json` | Isolated packaged binary, memory/RocksDB, worker/controller loss, 6,000 rows; not large-state, full application or 24-hour qualification. |
| Producer/consumer deltas: four pairs passed | `target/native-produced-envelope-delta-admission-v14b/` | Eight SQL captures, actual transported JSON, typed deltas and checkpoint recovery; independently audited values, frozen files and checkpoint hashes. Not full-profile parity or atomic checkpoints across jobs. |
| Quiet-key witness: failed validation | `target/native-quiet-key-window-witness-admission-v14b/` | SQL child passed; witness validator incorrectly expected a timestamp string instead of Debezium Unix milliseconds. Offline capture analysis confirms peer closure at 17:13:28 during both holds, while target count remains 1 rather than required zero. This query still fails. |
| Dynamic latest-window JOIN: planner rejection | `target/native-dynamic-latest-window-join-admission-v14b/` | Exact error: `can't handle updating left side of join`. See the [SQL proposal](native-dynamic-window-composition-proposal.md); no guard bypass or new contract is approved. |
| Large SESSION fixtures: prepared only | `target/native-session-large-fixtures-admission-v14b/preparation.json` | Hot-key and many-key inputs exceed 10× the declared 50 MiB pool. No large SESSION runtime has run on this candidate. |

The quiet-window encoding diagnosis is saved in
`target/native-quiet-key-window-witness-encoding-audit-v14b/analysis.json`, with
hashes of the original snapshots and checked CDC continuity. Debezium's default
is explicitly `UnixMillis` in `crates/arroyo-rpc/src/formats.rs`; ordinary JSON
uses its separate default. Original failed run artifacts and validator are
unchanged. Both live-hold snapshots have lifetime count 3, recent count 1 and
peer window end 17:13:28; EOF advances the peer to 17:13:34 without producing
the required target zero. This closes the timestamp-encoding investigation,
not the rolling-zero gate. Re-running this unchanged query cannot qualify it.

### Remaining acceptance gates

| Ticket | Remaining work; passing slices do not close the ticket |
| --- | --- |
| STR-16 | Complete retained-state, output, checkpoint, cancellation and slow-consumer accounting coverage. |
| STR-17 | Current-source high-cardinality/hot-key recovery and bounded ranking; slicing an already materialized full array is insufficient. |
| STR-20 | Large SESSION runtime and full lifecycle/schema parity, including out-of-order input and restored open sessions. |
| STR-26 | Configuration, metrics, operational recovery/rollback and measured defaults. Private instrumentation proposals have not been applied or tested. |
| STR-28 | Complete external field/lifecycle coverage and explicit distinctions between native support, defects and policy differences. |
| STR-29 | Complete typed profile/session composition, rolling zero/expiry behavior and bounded rankings. Standard updating-input JOIN support is a demonstrated limitation. |
| STR-32 | Full external identity/profile/session parity; combined fixed-parallelism ≥10× capacity, hot keys, backfill, slow consumers and actual 24-hour live/fault qualification. |

STR-44 tracks optional immediate-first/per-group delayed aggregate emission
post-MVP. Existing periodic Arroyo flushing remains accepted for this milestone;
that deferral does not defer the other gates.

### Resume without repeating completed work

1. Audit and publish the existing verified candidate as a reviewable PR update;
   preserve its frozen source/build evidence and obtain CI for the published
   candidate. Do not claim existing PR CI covers dirty worktree changes.
2. Resume the prepared large SESSION qualification through the shared serialized
   build/run queue after checking disk, container state and source hashes. Do not
   recreate fixtures or restart completed waves just because old handles vanished.
3. Use the retained quiet-window diagnosis: encoding is understood and the
   required zero is still absent. Investigate native expiry/result composition
   without repeating the unchanged query. Discuss any required updating-result
   contract and RocksDB read-view lifetime
   change before implementing observable semantics or architectural changes.
4. Finish the remaining ticket gates before starting the full 24-hour run.
   Broker uptime, finite captures and older-revision capacity results cannot
   substitute for that run or establish milestone completion.

## Historical `d19c30e3` evidence and open gates

PR #6 at `d19c30e3` has seven successful CI checks. The generic native UUID,
closed-HOP reaggregation and lifetime/latest-result composition fixtures each
passed eight memory/RocksDB × source batch target 1/8 × controller/leader
captures on this source. Their strict value, CDC and fresh-worker recovery
artifacts are `target/native-uuid-snapshot-fixed/`,
`target/native-window-composition-snapshot-fixed/` and
`target/native-result-composition-snapshot-fixed/`, respectively. The latter
shows ordinary existing-SQL lifetime and latest closed-window composition; it
does not emit an autonomous zero for an idle key or qualify profile timing.

The external application-owned native identity query using the original
16-field source input passed eight captures and 16 initial/recovered strict
comparisons of its complete 19-field output at
`target/native-identity-snapshot-fixed/comparisons.json`. A separate
application-owned 14-field profile-core SQL probe passed all eight
backend/batch/protocol cases at
`target/native-profile-core-snapshot-fixed/comparisons.json`. It checks the
selected core field values and checkpoint/recovery prefixes, not the full
33-field profile, emission/debounce timing or 12-session behavior. Streamr
contains none of those application schemas or policies.

The unchanged standard-SQL profile probes at
`target/native-profile-standard-sql-text-sink-probes/planner-gaps.json`
confirm two distinct planner gaps: `LAG` in the proposed session query fails
with `Window functions require already windowed input`, and an updating left
side of the daily rollup join fails with `can't handle updating left side of
join`. An aliased `DATE` grouping variant passed one memory/batch-1/controller
runtime and application-owned strict comparison at
`target/native-profile-standard-sql-aliased-day-probe/`. The initial
two-row CDC stream ends at October 3/4 counts 2/5; the committed two-row
checkpoint prefix ends at 2/1; the recovered three-row stream ends at 2/5. This
narrow successful grouping does not resolve the join, session transition,
idle-calendar or full-profile gates; its wider matrix remains untested.

At `target/native-tumble-capacity-snapshot-fixed-65000/`, a 65,000-key ×
8,192-byte RocksDB TUMBLE controller run checkpointed after 64,999 real rows,
restored in a fresh worker and compared all 65,000 keys and complete payloads.
The checkpoint retained-payload floor was 532,471,808 bytes, above 10× the
declared 50 MiB pool sum, and peak whole-child RSS was 355,414,016 bytes.
The leader run on that `d19c30e3` source failed after 551.74 seconds at
checkpoint publication: `disk checkpoint file metadata exceeds 3 MiB RPC
limit`. That failure remains part of the historical evidence.

A subsequent uncommitted batch shortened the immutable checkpoint-file
basenames and added opt-in bounded idle-output capture to the external SQL
test harness. The Bookworm logs
`/tmp/streamr-m3-compact-checkpoint-idle-{fmt,check,clippy,units,build}.log`
record passing formatting, workspace all-target check, strict Clippy, 25
library suites (614 passed, four ignored) and full workspace build. Its SQL
test binary had SHA-256
`5c9ffa95810b282a3de29d6afa9ff342da668aa9025960b3cf8fabae85a68aa4`.
On this source, `target/native-tumble-capacity-compact-leader-65000/` passed
a RocksDB leader checkpoint after 64,999 real rows, fresh-worker recovery,
and exact comparison of all 65,000 keys and complete 8,192-byte payloads.
Its checkpoint retained-payload floor was 532,471,808 bytes, above 10× the
declared 50 MiB pool sum; peak whole-child RSS was 361,074,688 bytes,
below the 512 MiB fixture cap. The earlier controller run and this leader
run qualify this selected capacity shape in both checkpoint protocols on
their respective tested source revisions. The 3 MiB metadata RPC cap and
other resource guards remain in place.

A later test-only pre-idle JSON Pointer/value readiness option passed another
Bookworm formatting, workspace all-target check, strict Clippy, 25 library
suites (616 passed, four ignored) and full build at
`/tmp/streamr-m3-idle-pre-match-{fmt,check,clippy,units,build}.log`. The
65,000-key leader result above used the earlier binary; it is not runtime
evidence for this later harness option. HOP capacity, quiet-key behavior,
application timer/profile parity, packaged faults, backfill and 24-hour
qualification remain open; milestone 3 is incomplete.

## Subsequent application-owned SQL diagnostics

The opt-in idle harness enabled an application-owned, three-real-event probe
of selected native branch retention. All eight memory/RocksDB ×
controller/leader × positive/negative cases at
`target/native-profile-idle-ttl-pre-match/runtime-results.json` completed
and passed the external strict comparator
`/tmp/native_profile_branch_ttl_capture.py` (SHA-256
`d0cce0093640d56c0ba0dd15f6586eda21dad6d3d215a75ba4c5d481bd6dea53`).
In both initial and fresh-worker recovered phases, complete CDC snapshots
showed `{lifetime_count: 3, recent_count: 2}` before an eight-second
live-source pause and `{3, 0}` after it with indefinite lifetime state;
the all-four-second-TTL negative control ended with deletion. The caller
checked byte-prefix continuity, typed before images and that both snapshots
preceded source EOF. This is processing-time TTL evidence for the selected
query. It does not prove event-time decay, UTC calendar rollover, Flink
timers, an autonomous idle-key zero for every SQL shape, or full profile
timing.

At `target/native-profile-rank-standard-sql/planner-runtime-results.json`,
three unmodified, application-owned existing-SQL diagnostics ran in
memory/controller/batch-1. Lifetime page and feature `COUNT` feeding
`ROW_NUMBER` both failed planning with `Window functions require already
windowed input`; a separate already-ranked `TEXT[]` `ARRAY_AGG` probe
failed native aggregate construction with `no bounded retraction index
codec`. These are precise implementation gaps, not a ranking value or
recovery pass and not a reason by themselves to add SQL syntax.

The application-owned keyed session cursor `MERGE` flags query passed all
eight memory/RocksDB × batch 1/8 × controller/leader captures and strict
initial/recovered comparisons at
`target/native-session-cursor-current/flags-runtime-results.json`: source
IDs `A,A,B,A,NULL,empty,A` produced flags `1,0,1,1,0,0,0`, with three
committed rows after the checkpoint prefix. Its separate native
`SUM(session_started)` totals variant failed planning on the first
memory/batch-1/controller attempt with `unsupported ArrowKey between related
state accesses`; no totals runtime matrix was qualified. The passing flags
show per-row state-table `RETURNING` behavior, not inactivity closure,
session timers, complete sessions or full profile parity.

A later generic fusion repair retained unrelated downstream aggregate consumers
outside the serial state-table owner while preserving rejection of unsupported
operators between related state accesses. On that source, the Bookworm logs
`/tmp/streamr-m3-native-boundary-{fmt,check,clippy,units,build}.log` record
passing formatting, workspace all-target check, strict Clippy, 25 library
suites (619 passed, four ignored) and full workspace build. The SQL test
binary had SHA-256
`aa1509e362b9ecbcfd96de050bca7aace96f58efbc0284a01c3d15380b74a248`.
The parsed `MERGE → flags → SUM` plan and structural fanout/forbidden
inter-access ArrowKey regressions passed. All 16 application-owned cursor
captures and strict initial/recovered comparisons then passed across
memory/RocksDB × batch 1/8 × controller/leader × flags/totals at
`target/native-session-cursor-fusion-fixed/runtime-results.json`. The
flags remained `1,0,1,1,0,0,0`; the native `SUM` plus `MAX(arrival_seq)`
totals query reached `(total_sessions=2, last_arrival_seq=3)` at the
checkpoint prefix and `(total_sessions=3, last_arrival_seq=7)` at EOF. The previous ArrowKey rejection is a historical failure,
not a current blocker for this selected query. The comparator is
application-owned (`/tmp/arcstream-native-session-cursor/native_session_cursor_capture.py`,
SHA-256 `b80ea4b4b548f3af1af095c7acd78300d4c03505160132b7c20e579efa57904f`);
it checks exact flags and typed CDC before images while allowing only valid
coalesced increasing source prefixes. This does not establish inactivity
closure, timer semantics, complete session records or full profile parity.

Four fresh small HOP captures also passed on this fused-source binary at
`target/native-hop-capacity-smoke-fusion-fixed/measurements.json`, covering
memory/RocksDB × controller/leader with eight 64-byte item payloads. The
full-payload oracle required both exact overlapping windows per item in
initial and fresh-worker recovered outputs (16 rows each). The checkpoint
retained-payload floor was only 448 bytes; this is a value/recovery smoke,
not a 10× HOP capacity qualification.

## Published fusion source and stronger HOP controller evidence

All seven PR #6 CI checks passed on published commit
`f58be0845a9ced64f3eeab75367709441258e83b`. This includes the compact
checkpoint filenames, idle harness and fusion repair described above.
The stronger HOP RocksDB controller run at
`target/native-hop-capacity-fusion-fixed-65000-controller/measurements.json`
then passed with the same `aa1509e3…` executable and source inventory. It
checkpointed after 64,999 of 65,000 real rows, retaining a conservative
532,471,808-byte payload floor, above 10× the configured 50 MiB pool sum.
Both initial and fresh-worker recovered streams matched all 130,000
item/window pairs and their complete 8,192-byte payloads. Whole-child peak
RSS was 355,061,760 bytes, below the 512 MiB cap; runtime was 1,339.62
seconds. On the later `8649bf440fd55e4cf4289b2bbc72969348b821f7`
source, the RocksDB leader attempt at
`target/native-hop-capacity-timestamp-fixed-65000-leader/hop-rocksdb-leader/runtime.log`
reported 130,000 initial output rows, checked by count only. Checkpoint then
failed with exit 101: `disk checkpoint file metadata exceeds 3 MiB RPC limit`
for the window state table. There was no recovered output or qualified
full-payload comparison. Strong HOP leader qualification remains pending.
This selected shape does not establish arbitrary window collections,
backpressure, application parity, packaged faults, backfill or the 24-hour gate.

## Earlier combined-source STR-29 evidence

The `fc347312`-based batch, incorporated into `d19c30e3`, added internal
serial-owner point reads for native aggregate chunks without indexed
accumulators, one stable native-window snapshot per watermark pass, and bounded append-only unordered FIRST/LAST. It also
propagates a nested constructor error instead of accepting it as an empty
result. These are existing-engine implementation repairs; they add no SQL
syntax, public backend method, or application policy.

The Bookworm container logs
`/tmp/streamr-m3-native-first-snapshot-fixed-{fmt,check,clippy,units,build}.log`
record passing formatting, workspace all-target check (16.48 seconds), strict
Clippy (18.85 seconds), 25 library suites (610 passed, four ignored, none
failed), and full workspace build (57.26 seconds). The SQL test executable was
`arroyo_sql_testing-df802a90a8396c4e` (SHA-256
`b38ee120315ed1490d9bf96de23de44f6f6e697be8809c52eab42f07bbe35364`).

On this same source, all 16 native current-result retention captures passed:
eight ten-event retraction cases across memory/RocksDB, source batch targets
1/8 and controller/leader checkpoints, and eight many-window cases across
memory/RocksDB, 64/4,096 groups and controller/leader at batch target 8. The
strict CDC/recovery fixture checks before images, checkpoint-prefix values and
final values, including a recent-count 2-to-1 replacement. Artifacts and the
SHA-256 comparison manifest are at
`target/native-result-retention-snapshot-fixed/`. The selected aggregate
checkpoint inventory has the same 11 keys and 4,708 stored value bytes at 64
and 4,096 groups in all four backend/protocol pairings. This count excludes
window/source/sink state, RocksDB overhead and RSS. The prior 120.24- and
900.27-second RocksDB/controller timeouts remain historical failures; the
4,096-group RocksDB controller and leader cases now completed initial and
recovered captures in 166.55 and 143.94 seconds total, respectively. These are
end-to-end observations, not a controlled CPU speedup measurement. See
[native result composition](native-result-composition.md) and
[checkpoint inventory](native-checkpoint-inventory.md) for the exact scope.

All eight unordered FIRST/LAST captures passed across memory/RocksDB, source
batch targets 1/8 and controller/leader checkpoints at
`target/native-unordered-first-last-fixed/`. This does not qualify every
ordered, FILTER, collection, or retraction variant. Full 33-profile and
12-session gates remain open; the 10× TUMBLE capacity run on this source is
pending. At this earlier validation point, a 14-field application SQL
proposal was stored in the application repository but had not yet been executed.
No application-specific definitions were added to Streamr.

## Current STR-28 evidence gate

### Uncommitted combined native engine batch

The subsequent batch based on `fc347312` adds reviewed SESSION cache/paged
retirement, finalized-window projection support, the existing STR-37 Kafka
recovery implementation, bounded append-only MIN/MAX/ordered FIRST/LAST state,
and admission of the existing native `uuid()` function as a bounded projected
value. No new SQL syntax is introduced by these repairs.

Bookworm formatting, workspace all-target checks (31.78 seconds) and strict
Clippy (34.97 seconds) passed. The first library run stopped at one UUID
negative-test diagnostic mismatch; the planner correctly rejected the volatile
primary key. After independently reviewing the one-line assertion correction,
the retry passed 604 library tests with four explicitly ignored, and the full
workspace build completed. Container logs are
`/tmp/streamr-m3-native-combined-{fmt,check,clippy,units}.log`,
`/tmp/streamr-m3-native-combined-{fmt,units}-retry.log`, and
`/tmp/streamr-m3-native-combined-build.log`.

The fresh executable passed eight closed-HOP-to-updating-aggregate captures
with strict initial/recovered CDC value and before-image assertions across
memory/RocksDB, source batch targets 1/8 and controller/leader checkpoints.
Artifacts are `target/native-window-composition-combined`. This qualifies
ordinary window-result reaggregation, not lifetime/current-result joins or
quiet-key zero emission. The first native UUID runtime probe completed its
checkpoint and restore but its oracle incorrectly compared random IDs from
two independent fresh runs. After independently reviewing that fixture-only
correction, all eight native UUID captures passed across the same backend,
batch and protocol matrix. Both keys are inserted before the checkpoint and
repeated afterward; lookup results, dependent MERGE no-action values and
output branches agree with each run's own committed bindings. Artifacts are
`target/native-uuid-state-current`. This is a generic native UUID proof, not
application identity qualification without an extra candidate input field.
Full combined-source capacity, application parity, packaged faults, backfill
and 24-hour gates remain open.

On this combined executable, the external identity SQL then passed eight
captures and 16 strict initial/recovered business comparisons against the
application-owned Flink oracle: 13 unified events and two directed merges in
every phase, across both backends, batch targets 1/8 and checkpoint protocols.
The application now supplies only its original 16 fields; one native SQL
`uuid()` producer precedes the lookups/MERGEs, replacing the fixture-only
prepared candidate field. Query/preparation changes remain external.
Artifacts are `target/native-identity-uuid-combined` (checkpoint 41, prefix 10,
16 MiB execution budget). Whitespace/fault/capacity variants on this route and
packaged Kafka delivery still need qualification.

The 1,024-row SESSION RocksDB smoke and 65,000-row hot-session captures passed
both checkpoint protocols on this executable. Full pre-EOF retained payload
floor is 532,480,000 bytes, above 10 times the conservative 50 MiB pool sum;
peak whole-child RSS is 366,272,512 bytes (controller) and 358,846,464 bytes
(leader), below 512 MiB. The oracle checks exact count, window and complete
first payload in both initial/recovered outputs, not every raw input payload.
The checkpoint prefix is only 32,500 rows (266,240,000 payload bytes), so this
does not qualify 10x checkpoint/export/restore. Artifacts are
`target/native-session-capacity-combined-{smoke,65000}`. A reviewed generic
`--checkpoint-rows` option permits 64,999 retained rows with one suffix row.
That stronger run exited successfully for both protocols at
`target/native-session-capacity-combined-checkpoint-65000`. The checkpoint
retained-payload floor is 532,471,808 bytes, also above 10 times the 50 MiB
pool sum. Whole-child RSS peaks are 361,672,704 bytes (controller) and
365,682,688 bytes (leader); initial and fresh-worker recovered outputs match
the count/window/full-first-payload oracle. This qualifies that hot-session
checkpoint shape, not high-cardinality sessions, arbitrary raw payload parity,
backfill, packaged Kafka faults or the 24-hour gate.

The subsequent four-owner capture harness batch passed formatting, workspace
all-target checks (37.73 seconds), strict Clippy (39.90 seconds), all 604 library
tests (four ignored, 25 suites) and the full workspace/all-target build
(3 minutes 48 seconds). Logs are
`/tmp/streamr-m3-four-owners-{fmt,check,clippy,units,build}.log` inside the
development container. This compiles the test-only owner-limit overrides;
the native lifetime/latest-window UNION value/recovery matrix is separate.

The generic UNION nullability repair passed its branch-order/non-null control
regression and all 605 library tests, strict Clippy, workspace checks and build.
After a test-only 4 MiB scan-page override for four owners, the final combined
batch again passed formatting, checks (4.38 seconds), Clippy (2.19 seconds),
605 tests (four ignored) and full build (8.86 seconds). Logs are
`/tmp/streamr-m3-composition-scan-pages-{fmt,check,clippy,units,build}.log`.
Four memory composition captures passed on the preceding executable; the
subsequent run encountered a valid extra initial CDC update and failed its
exact row-count assumption before RocksDB cases ran. See
`docs/native-result-composition.md`; full composition qualification remains
pending. The read-only checkpoint inspector passed synthetic corruption checks
and inspected all four actual memory prefix checkpoints successfully, without
establishing many-window retention or application emission parity.

### Uncommitted checkpoint/window/lineage batch

The combined batch based on `fc347312` passed Bookworm formatting, workspace
all-target checks (56.50 seconds), and strict Clippy (64 seconds). Its first
workspace library run failed four Kafka connector tests: the isolated
`streamr-m3-test-kafka` broker had exited with a JVM SIGBUS and admin requests
timed out. Restarting that test broker restored topic-list access. The retry
then failed two new planner timestamp-lineage tests (an unavailable source
column and an incorrect assumed output layout). Neither failed run establishes
full-batch acceptance.

After independently reviewed fixture corrections and direct qualified-schema
coverage for middle/appended timestamp layouts, the final combined run exited
zero: formatting, all-target checks (31.38 seconds), strict Clippy (34.07
seconds), 582 library tests (four ignored), and full workspace build (61
seconds). Logs in the development container are
`/tmp/streamr-m3-perf-metrics-lineage-{fmt,check-retry,clippy-retry,units-final,build-final}.log`.

The fresh `arroyo_sql_testing-df802a90a8396c4e` then ran an externally supplied
native identity query with memory state, controller checkpoints, source batches
of eight and 16 MiB execution accounting. Initial and checkpoint-41 recovered
captures each contained 13 rows; the application-owned adapter/oracle compared
all 13 unified events and two directed identity merges successfully in both
phases. Artifacts are `target/native-identity-current`, with runtime log
`/tmp/streamr-native-identity-current-retry.log` in the development container.
This is one backend/protocol/batch combination, not full identity qualification
or profile/window composition evidence. Other runtime matrices, capacity runs
on this final source, and milestone acceptance gates remain open.

The expanded external native identity matrix then passed eight captures and
16 strict business comparisons: memory/RocksDB × source batches 1/8 ×
controller/leader, each with initial and checkpoint-41 recovered output. Every
phase matched 13 unified events and two directed identity merges through the
application-owned oracle. Artifacts are `target/native-identity-matrix-lineage`.
Whitespace, injected mutation failures, Kafka/process faults and capacity are
not covered by this 13-event matrix.

A separate whitespace variant then passed eight captures and 16 strict
initial/recovered comparisons on this same earlier source. It replaces every
`alice` user ID with a space, tab and space, and applies the same bijective rename to the
baseline reference expectations. This checks nonempty whitespace handling,
not a new execution of the Flink oracle. Artifacts are
`target/native-identity-matrix-whitespace`; later combined-source qualification
is still required.

On the same source, the scalar, ordered and collection/UNNEST window scripts
passed all 48 initial/recovered capture cases across both configured backends
and checkpoint protocols; the oversized collection case rejected its output
before final execution as expected. Logs are
`/tmp/streamr-m3-first-partial-{windows,ordered,collections,oversize}.log` in the
development container, with matching `target/native-*first-partial` artifacts.
The ordered fixture requests 128-row source batching and checkpoints 70 input
rows; these logs do not observe the actual operator batch length. Full capacity
and session qualification on subsequent source changes remain separate gates.

### Current foundation batch

The foundation batch committed at `ad90cee9` on top of `d59124ea` passed these
Bookworm library runs through the shared machine build queue:

```sh
/home/jason/repos/streamr/scripts/rust-build podman exec \
  -e CARGO_TARGET_DIR=/app/target/milestone2-runtime \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  streamr-state-build cargo test --locked -j4 -p arroyo-state --lib

/home/jason/repos/streamr/scripts/rust-build podman exec \
  -e CARGO_TARGET_DIR=/app/target/milestone2-runtime \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  streamr-state-build cargo test --locked -j4 -p arroyo-worker --lib
```

The state run passed 68 tests, including memory/RocksDB/third-adapter typed-table
conformance, allocation guards, exact resource admission, and real Parquet
metadata preservation and legacy rejection across both compaction partition
paths. The worker run passed 73 tests, including ordered values, FILTER and NULL
inputs, retractions, duplicate ordering keys, serialized-row reload into a fresh
operator, and state-table preflight rejection before task construction.

The planner run subsequently passed all 113 library tests using the same queue
and container command with `-p arroyo-planner --lib`. This covers retained-table
declarations, component-safe names, named MERGE effects and shared capture,
complete-key INNER/LEFT lookups, deterministic key expressions, related owner
compatibility and native aggregate/window plan probes.

These passes establish the tested foundation, planning and aggregate correctness
slices. Workspace all-target checking and strict Clippy passed for this batch
(`cargo check --locked -j4 --workspace --all-targets` and `cargo clippy --locked
-j4 --workspace --all-targets -- -D warnings`, exit 0). Logs are in the container
at `/tmp/streamr-m3-workspace-{check,clippy}.log`. Formatting and staged whitespace
checks also passed. The full workspace build subsequently passed at `ad90cee9`
(`cargo build --locked -j4 --workspace`, exit 0, 23m 23s; container log
`/tmp/streamr-m3-workspace-build.log`). STR-41 integration starts after this
source-frozen build; its subsequent edits need separate verification.
All seven reported PR checks passed on full head
`ad90cee99a99bdb78fcd4352eefe6c9e47d13645`; the head was rechecked after the CI
watch exited 0. The [CI run](https://github.com/jasadams/streamr/actions/runs/37159026844)
passed its build and strict Clippy steps, 521 tests with four skipped, and all
12 integration tests across four runs. Its local log is
`/tmp/streamr-m3-ci-ad90.log`. This is foundation evidence, not acceptance of
the subsequent STR-41 integration edits.
At that foundation head, STR-40 planning deliberately rejected startup pending
STR-41's fused serial execution, exercised in the subsequent batch below.

### Native state-table execution and recovery

The subsequent STR-41 integration batch builds on foundation `ad90cee9`.
Its latest combined Bookworm library run passed 332 tests: planner 124,
state 69, protocol 59 and worker 80 (container log
`/tmp/streamr-m3-native-runtime-units.log`). Worker regressions invoke the actual
fused owner's `process_batch`: a single row emits without further input/control,
a three-row hot-key/null-key batch splits under a one-row pending budget,
an event budget error releases its scope, and a blocked collector keeps the
decoded pool charged until cancellation releases its transient permits.

The SQL-testing executable rebuilt successfully with `CARGO_INCREMENTAL=0`:
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832`.
The reproducible generic driver is [scripts/test-native-state-tables.py](../scripts/test-native-state-tables.py).
Run it inside the Bookworm container through the shared machine queue:

```sh
python3 /app/scripts/test-native-state-tables.py \
  /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832
python3 /app/scripts/test-native-state-tables.py \
  /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-7c49ff5580181832 \
  --directory /app/target/native-state-table-timestamp-fixtures --target-timestamp
```

Both suites passed on memory/RocksDB, source batch targets 1/8 and controller/
leader checkpoint publication: 16 runs and 64 complete JSON output comparisons.
Each uses 15 generic events, two dependent named MERGEs, a current-row LEFT
lookup, an intermediate sink and two final consumers. Assertions cover repeated
and interleaved composite keys, Unicode/empty key components, NULL keys,
matched/absent no-actions, delete/reinsert and a deliberately invalid arithmetic
expression in an unselected clause. A checkpoint after four events at epoch 1
is published, workers are recreated, and initial/recovered/mirror/intermediate
outputs match independent value oracles. The second suite additionally stores a
BIGINT value named `_timestamp` and selects it as `stored_marker`; event time
retains its separate identity. Unaliased retained/computed `_timestamp` output
is explicitly rejected in the current subset, as documented in
[the SQL contract](state-tables.md).

Artifacts are under `target/native-state-table{,-timestamp}-fixtures`; logs are
in the container at `/tmp/streamr-native-state-table{,-timestamp}-fixtures-*.log`.
Batch sizes 1/8 are source configuration targets; the direct callback regression
independently exercises an actual three-row RecordBatch. These small captures
do not establish beyond-RAM behavior, all fault points, backend switching, or
STR-32's backfill/live gates. The integration source is committed at `67d9c96a`.
Its full workspace build passed (3m 10s, container log
`/tmp/streamr-m3-final-build.log`), and strict workspace all-target Clippy
passed (1m 02s, `/tmp/streamr-m3-final-clippy.log`). Both 16-case capture suites
above were rerun successfully after the final timestamp fix. All seven reported
PR checks passed on full head `94f6228e87ea98f213c17973cbbb9433eb3033b8`;
the head was rechecked after CI completed. The
[CI run](https://github.com/jasadams/streamr/actions/runs/37167544579)
covers this committed integration source: 543 tests passed with four skipped,
and all 12 integration tests passed across four runs. Subsequent native aggregate and
capacity/recovery additions are separate work and require their own gates.
The workspace all-target check and strict Clippy passed for this integration
batch. The workspace library run initially failed six Kafka/MQTT connector
tests because local brokers were absent. With dedicated Kafka 3.9.2 and
Mosquitto test brokers running in the build container's network, its unchanged
workspace connector executable passed all 60 tests (container log
`/tmp/streamr-m3-final-connectors.log`). The final timestamp
projection regression also passes the planner's 125-test library suite.

### State-table retained-capacity and selected-checkpoint recovery

The generic driver [scripts/test-state-table-capacity.py](../scripts/test-state-table-capacity.py)
passed with the SQL executable built from `67d9c96a`, using RocksDB and both
controller and leader checkpoint publication. Each run inserts 65,000 keys with
8,192 bytes of deterministic, varied payload per key, then probes 64 keys spread
across the retained set. Retained payload totals 532,480,000 bytes, more than ten
times the conservative 48 MiB sum of the fixture's executor/live resource limits.
The process RSS envelope is separately declared as 512 MiB.

Controller peak RSS was 296,431,616 bytes; leader peak RSS was 292,675,584 bytes,
measured with Linux `wait4` for the complete worker process. Both runs compare
all 65,064 output rows before and after fresh-worker restore, including old/new
values, actions and current lookup payload equality. The selected checkpoint is
epoch 1 after 32,500 input rows. Four complete comparisons passed in total.
Artifacts and measurements are under `target/state-table-capacity-varied`.
A 100-key smoke run also passed both modes and explicitly did not meet the
ten-times-pool threshold.

This measures logical retained payload and actual process RSS. It does not claim
that physical compressed storage exceeds host RAM, or qualify native aggregates,
windows, backend switching, every failure point, or the full backfill/live gates.
The separate backend-switch checkpoint unit test subsequently passed as part
of the 70-test state library suite. It exercises all four source/destination
backend pairs across full, updated/deleted and empty epochs, incomplete restore
rejection and fresh-attempt retry. Integration build/test gates passed as recorded
below. The subsequent cleanup fix at `4ffac3ff` passed all seven CI checks,
as recorded below.

### Configured native updating-aggregate state

The subsequent STR-17 batch adds `worker.aggregate-state` limits and uses the
configured generic live backend for group accumulators, counted extrema,
ordered members, dirty output state and finite-retention expiry. Memory and
RocksDB use the same SQL/operator code. `SET updating_ttl = NULL` explicitly
requests indefinite aggregate retention; default and finite retention remain
compatible, and join retention remains finite. Ordinary admitted backend batches
and checkpoint barriers provide visibility and recovery; no per-event durable
transaction or synchronous flush is added.

The complete Bookworm workspace library run passed 561 tests, including planner
132, state 70, protocol 59 and worker 90. Formatting, strict workspace all-target
Clippy (36.54s), and the full workspace build (2m 31s) passed. Logs are
`/tmp/streamr-m3-native-aggregate-{fmt,clippy,build,final-units}.log`.
The previous all-target compile check also passed (1m 42s); its log is
`/tmp/streamr-m3-native-aggregate-check.log`.

The generic [native aggregate driver](../scripts/test-native-aggregates.py)
passed eight configured memory/RocksDB × source batch target 1/8 × controller/
leader cases. Its actual SQL composes a per-item MAX aggregation with a second
COUNT/FILTER/SUM/MIN/MAX/ordered FIRST IGNORE NULLS/LAST aggregation, so the second
stage consumes genuine updating-input retractions. It compares full initial,
checkpoint-prefix and recovered values and checks each Debezium before/after
transition. A four-input-row epoch-1 checkpoint precedes fresh-worker restore.
The recovered sink contains two creates and two updates; each update encodes its
before and after rows in one Debezium record.

The optional `--baseline` comparison reproduces an existing legacy moving-MAX
defect after restore/retractions: one group reports 7 instead of the correct 9.
The native counted index returns 9. That negative comparison is retained as
evidence, rather than used as the native value oracle. SQL with equal ORDER BY
tuples also permits ambiguous LAST results; the existing operator demonstrated
different initial and recovered tie choices. Exact native value assertions use
an explicit total-order tie-breaker.

The final-source rerun passed all eight native cases, followed by both generic
state-table suites (16 cases and 64 full comparisons). The reproducible
[ordering diagnostic](../scripts/test-native-aggregate-ordering.py) also passed
four native/legacy memory cases with `--total-order` at source batch targets
1/8, comparing exact initial, checkpoint and recovered values. Its default
equal-order mode records permitted tie differences explicitly rather than
requiring the existing operator's inconsistent choices. Logs are
`/tmp/streamr-m3-native-aggregate-{final-matrix,total-order,equal-order}.log`
and `/tmp/streamr-m3-native-tables-aggregate-{batch,timestamp}.log`.

These small operator captures and units do not qualify aggregate hot-key/high-
cardinality capacity, every failure point, rescaling, native windows or the full
backfill/live gates. The subsequent cleanup fix at `4ffac3ff` passed all seven
CI checks, as recorded below.

### Checkpoint cleanup and live RocksDB directory races

At `4ffac3ff90a94bedb179dd38256e0f7a512bdc96`, all seven PR checks passed,
including both Rust jobs. The exact head was rechecked after the CI watch exited
0. [Rust CI](https://github.com/jasadams/streamr/actions/runs/37175698196)
passed after the backend-switch test awaited its cleanup executor before process
exit. One Rust job on the preceding `aea39170` head had passed the test but then
aborted during native cleanup; that preceding head was not fully green.

Live RocksDB capacity scanning now tolerates child files disappearing during
compaction, while a missing database root and other I/O failures remain errors.
The final local workspace library run for this fix and the scalar/ordered native
window draft passed 566 tests. Five separate processes each passed the exact
backend-switch checkpoint test and exited 0. These results address cleanup and
mutable-directory races; they do not complete the operator fault or capacity
matrices.

The native fixed-window draft passed 20 scalar and 20 ordered SQL captures:
TUMBLE/HOP, configured memory/RocksDB, both checkpoint protocols and variable
source batching, plus legacy memory comparison cases. Each capture checks full
initial and fresh-worker recovered values. The ordered fixture uses 78 rows and
checkpoints after 70; actual debug traces show 70 retained rows split into two
partial chunks. Both existing `hourly_by_event_type` and `sliding_window_end`
goldens passed on each backend. Artifacts are `target/native-windows-final`,
`target/native-windows-ordered-final` and container logs
`/tmp/streamr-m3-native-window-{final,ordered-final}-matrix.log`.

These window results precede the subsequent collection, SESSION and centralized
backend-construction edits. The combined edits passed formatting, workspace
all-target checking, strict Clippy and 572 workspace library tests; their runtime
SQL and full build qualification is separate and still in progress. STR-19 and
STR-20 remain In Progress, with quiet-key, late/idleness, capacity, backpressure
and fault acceptance still open.

### Combined native windows, SESSION and backend construction

The reviewed combined draft uses one configured backend construction adapter
for aggregate, fixed-window, session and state-table owners. The final collection
repair passed `cargo fmt --all -- --check`, workspace all-target checking, strict
all-target Clippy, all 573 workspace library tests (four ignored), and a full
workspace build. Container logs are
`/tmp/streamr-m3-window-collection-repair-{fmt,check,clippy,units,build}.log`.
The fresh SQL executable is
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-df802a90a8396c4e`.

The [collection driver](../scripts/test-native-window-collections.py) passed 16
memory/RocksDB × controller/leader × source batch target 1/128 captures of
ARRAY_AGG, ARRAY_AGG(DISTINCT), COUNT(DISTINCT) and single-column UNNEST. Full
initial/recovered multisets preserve duplicates and NULL values. The 4096-row
negative case rejected the oversized array before final aggregation, rather
than truncating it. Final output admission counts logical flat payload bytes;
working admission still counts decoded array allocations and aggregate state.
Artifacts: `target/native-window-collections-repaired`; container log
`/tmp/streamr-m3-native-window-collections-repaired.log`.

The [SESSION driver](../scripts/test-native-sessions.py) passed eight delayed-
watermark cases across both backends, protocols and source batch targets 1/8.
It compares three complete session rows, including equal-gap joins and an
out-of-order bridge, before and after an open-session checkpoint and fresh-worker
restore. Its `--late-input` mode passed four direct-watermark cases at batch
target 1: four rows below the advanced watermark are dropped and four exact
singleton session rows remain. Watermark progression occurs between delivered
batches; this late-drop oracle is intentionally not asserted for batch target 8.
The default bridge test retains the same three-row oracle across batch targets.
Artifacts: `target/native-sessions-{delayed,late}-combined`; logs
`/tmp/streamr-m3-native-sessions-{delayed,late}-combined.log`.

On this same final collection source, both the scalar and ordered window
matrices passed their 16 native memory/RocksDB × protocol × batch cases again.
Logs are `/tmp/streamr-m3-native-window-collection-source.log` and
`/tmp/streamr-m3-native-window-ordered-collection-source.log`. The earlier
legacy comparison and existing golden passes are recorded separately above.

The [legacy equality driver](../scripts/test-legacy-session-equality.py) passed
all four batch target 1/128 × controller/leader cases. Input times 0, 10, 12
with gap 10 produce one `[0,22)` window with count 3 initially and after restore.
This verifies the sorted-prefix/end-update repair against the existing native
SESSION equality rule; it does not change an external application's lifecycle.

After backend construction was centralized, native aggregates passed all eight
value/recovery cases again. Both state-table suites passed their combined 16
cases and 64 full output comparisons. Logs are
`/tmp/streamr-m3-native-aggregate-factory-combined.log` and
`/tmp/streamr-m3-native-tables-factory{,-timestamp}-combined.log`.

These captures do not establish larger-than-budget window/session state,
cancellation/backpressure, idleness, session collections, current rolling zero
output or the full fault/live gates. UNNEST here projects one flat column; it
does not qualify unrestricted expansion of companion columns. The actual
STR-29 composition planner rejections and complete SQL are saved in
[native result composition](native-result-composition.md). Subsequent CI must
validate the committed combined head separately from the preceding `4ffac3ff`.

### Native window/session capacity fixture and streaming capture

The capture harness now validates one JSON object at a time instead of retaining
an entire output file. This avoids counting a 500 MiB capture file as retained
engine state during RSS qualification. The reviewed helper passed formatting,
workspace all-target checking, strict Clippy and all 573 library tests. Logs:
`/tmp/streamr-m3-streaming-capture-{fmt,check,clippy,units}.log`.

The [capacity driver](../scripts/test-native-window-capacity.py) passed eight
small 8-row TUMBLE/SESSION × memory/RocksDB × controller/leader captures using
8 KiB varied payloads and full checkpoints. These are fixture smoke tests,
not larger-than-budget evidence. TUMBLE compares every key and full payload;
SESSION compares its one emitted count/window/FIRST payload and does not expose
every intermediate retained payload.

At 65,000 rows, the proposed full open-state payload is 532,480,000 bytes
(507.8 MiB), exceeding ten times the conservative 50 MiB fixture pool sum. The
halfway checkpoint contains only 266,240,000 payload bytes (253.9 MiB); that
checkpoint must not be described as 10×. These figures describe fixture admission goals, not a passed capacity result.
Physical compressed disk size is a separate quantity.


### Window snapshot reuse: correctness passed, capacity recovery still open

The working-tree window repair on top of `fc347312` reuses one stable snapshot
for a window's group traversal and one for an expiry traversal, rather than
creating physical RocksDB checkpoint snapshots repeatedly within those loops.
Expiry writes remain bounded and recheck the live catalogue to preserve groups
with future panes. Independent review found no blocker. Formatting, workspace
all-target checking, strict Clippy, all 574 library tests and the workspace build
passed; container logs are `/tmp/streamr-m3-window-closure-{fmt,check,clippy,units,build}.log`.

The repaired binary passed 16 scalar, 16 ordered and 16 collection/UNNEST
captures, with exact initial and recovered outputs across memory/RocksDB and
controller/leader checkpoint modes. The oversized collection was rejected before
final execution as expected. Artifacts are `target/native-windows-snapshot-reuse`,
`target/native-window-ordered-snapshot-reuse` and
`target/native-window-collections-snapshot-reuse`.

The full 65,000-row RocksDB/controller TUMBLE run at
`target/native-window-capacity-reused-snapshot` timed out after 900 seconds during
recovery. Its uninterrupted initial output separately passed the complete
65,000-key, full-payload oracle; the interrupted recovered output contained
49,258 rows. This is **not** a passed recovery, RSS or full capacity gate. A live
observation of peak RSS was below 512 MiB, but no completed child measurement was
returned. The earlier full attempt was deliberately terminated because repeated
snapshot creation made little progress. Neither failed attempt establishes the
milestone's capacity acceptance.

The subsequent 1,800-second-per-capture retry passed both controller and leader
RocksDB TUMBLE modes. Each compared all 65,000 keys and complete 8 KiB payloads
in both uninterrupted and recovered output. Full retained payload was
532,480,000 bytes, above 10× the conservative 50 MiB pool sum; the halfway
checkpoint was only 266,240,000 payload bytes and is **not** a 10× checkpoint.
Whole-child peak RSS was 361,066,496 bytes (controller) and 359,043,072 bytes
(leader), both below the declared 512 MiB limit. Capture elapsed times were
876.81 and 1,020.59 seconds respectively; the timeout was extended without
changing counts, values or memory assertions. Artifacts and measurements:
`target/native-window-capacity-snapshot-reuse-1800/measurements.json` and each
case's `runtime.log`, `output.initial.jsonl` and `output.jsonl`.

This qualification used `fc347312` plus the first snapshot-reuse/expiry repair,
before the later first-partial reuse and checkpoint metrics changes. The latter
changes need their own combined checks and runtime qualification. SESSION
capacity, broader failure points and representative backfill/live gates remain
open.

### Native aggregate value and recovery capture

The SQL-testing binary rebuilt from this batch with `cargo test --locked -j4
-p arroyo-sql-testing --no-run` (exit 0; container log
`/tmp/streamr-m3-sql-build.log`). The selected executable is
`/app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-b1c805b1e01cc8a7`.
Two isolated `external_sql_checkpoint_capture` runs passed with configured
`memory`, 16 MiB execution resources, checkpoint epoch 1 after two of three input
rows, and controller and leader checkpoint publication respectively. All four
complete initial/recovered JSONL comparisons passed.

The generic query under `target/native-aggregate-values/{controller,leader}` uses
these aggregate expressions grouped by `item_key`:

```sql
COUNT(*) AS total,
COUNT(*) FILTER (WHERE selected) AS selected_total,
FIRST_VALUE(item_value ORDER BY ordinal) AS first_value,
LAST_VALUE(item_value ORDER BY ordinal) AS last_value,
FIRST_VALUE(item_value ORDER BY ordinal DESC) AS descending_first,
LAST_VALUE(item_value ORDER BY ordinal) FILTER (WHERE selected) AS selected_last
```

Input `(ordinal, item_value, selected)` is `(1, 'u', true)`, `(2, 'v', false)`,
`(3, 'w', true)`, all under key `x`. The uninterrupted capture emits one create:
`(total, selected_total, first_value, last_value, descending_first, selected_last)
= (3, 2, 'u', 'w', 'w', 'w')`. Recovery preserves the checkpoint's create at
`(2, 1, 'u', 'v', 'v', 'u')`, followed by an update with that exact before-image
and the uninterrupted final after-image. The explicit null before-image is also
compared. Logs: `/tmp/streamr-native-aggregate-{controller,leader}.log`.

This exercises the native planner, operator, ordinary Arrow checkpoint tables,
publication, source/sink progress and recreated worker. The aggregate caches
remain in memory; STR-17's configured live-backend migration, bounded fallback
history and resource qualification remain open. It does not qualify native MERGE.

The [native capability audit](milestone-3-native-capabilities.md) records the
2026-10-04 source review against the full STR-28 ticket and all four substantive compatibility comments.
The revised application sketches, reference payload fields, planner guards,
worker admission, operator retained state and connector records were inspected.
No Cargo command or native physical-plan/value/recovery capture was run for this documentation slice. The coordinating
agent owns the combined Bookworm validation queue; pending results are not passes.


Remaining acceptance requires pinned application query/oracle revisions, actual
positive/negative native plan captures, exact ordered aggregate/FILTER/NULL and
collection values, approved lifecycle/ownership differences, configured-backend
migration with registered namespaces and admitted buffers, and restore/output
checks in memory/controller, RocksDB/controller and RocksDB/leader modes.
FIRST_VALUE/LAST_VALUE's reported input/order/filter defects are now covered by
the unit and memory-configured native recovery captures above; their live-backend
migration and broader acceptance remain open. Lifetime ranks, updating-input joins,
unequal-window composition and session ranking/join restrictions have source
evidence but still require actual plan probes and a discussed minimum contract
for any required gap. No public callback API is approved by this audit.

SESSION equality/next-instant deadlines, canceled/extended deadlines, old
arrivals, pre-watermark gap splitting, reused IDs, >=24-hour continuous activity,
quiet producers and restore around closure remain open. Idle output, calendar
expiry-to-zero, processing-time coalescing and last-emitted changed-field deltas
are not demonstrated by scalar sketches or timer storage tests. Source audit is
a partial STR-28 milestone, not STR-28 or M3 completion.

### Current composition and retention status

The bounded-capture source passed formatting, workspace all-target checking
(5.52 seconds), strict Clippy (2.34 seconds), 606 library tests with four
ignored across 25 suites, and full workspace build (9.59 seconds). Logs are
`/tmp/streamr-m3-bounded-capture-{fmt,check,clippy,units,build}.log`. On the
subsequent composition-queue batch, formatting, checking (2.58 seconds) and
Clippy (2.15 seconds) passed. Its concurrent library run failed one Kafka
metadata-source test after a timeout; the serial rerun passed all 606 tests
(four ignored), including all 67 connector tests. The SQL-testing smoke suite
within that run passed 43 tests with four ignored in 113.34 seconds. The full
workspace build passed in 8.06 seconds. Logs are
`/tmp/streamr-m3-composition-queue-{fmt,check,clippy,units,units-serial,build-serial}.log`.

The prior 32 MiB RocksDB queued-write attempt reached runtime: the job and all
operators started, then task 17 failed queued-write admission with
`ResourceExhausted` (`queued_write_bytes=33554432`) shortly after startup and
before initial capture/checkpoint. The three aggregate owners need five 2 MiB buffers
each and the window owner needs five 0.5 MiB buffers: 32.5 MiB before metadata.
A reviewed 64 MiB queue
override is confined to both generic test fixtures; production limits and source
guards are unchanged.

The current-result UNION capture passed all eight memory/RocksDB × source batch
target 1/8 × controller/leader cases with strict CDC, before-image, checkpoint-
prefix and final-value comparisons. The prefix has lifetime count 1 and recent
count NULL; final materialized values are lifetime count 3 and recent count 1.
Artifacts are `target/native-result-composition-queue-qualified`; log:
`/tmp/streamr-m3-composition-queue-runtime.log`.

A separate four-event retention probe passed memory/batch-1 controller and
leader checkpoint inspection: each of three owners had `G=1`, and the outer
aggregate had `M=4,R=4`. Its batch-8 companion expected recent count 2 but
observed NULL: the watermark generator takes the minimum event time in each
source batch, including the batch values `[1,3,5]`. That expectation is invalid
for this batch shape and does not demonstrate an engine defect.

The earlier retention qualification attempt contained 16 scopes: eight
ten-event retraction captures (memory/RocksDB × batch target 1/8 × controller/leader) and eight
many-window captures (memory/RocksDB × 64/4096 groups × controller/leader at
batch target 8). Fourteen scopes passed on that earlier source: all eight
retraction cases, all four many-window memory cases, and both 64-group RocksDB
cases. Their artifacts are
under `target/native-result-retention-tail-qualified`; the two then remaining
scopes were the 4096-group RocksDB controller and leader cases.

The checkpoint inventories for the 64- and 4096-group memory cases match across
cardinality and both checkpoint protocols. The three owners each have `G=1`;
the latest result stores 1,840 bytes, the lifetime aggregate 1,456 bytes, and
the outer aggregate 1,200 bytes plus `M=4` entries of 9 bytes and `R=4` entries
of 44 bytes. Total logical value bytes are 4,708 in each checkpoint. This
qualifies the measured many-window fixture's constant checkpoint shape across
64 and 4096 groups; it does not qualify arbitrary retention workloads.

An earlier 4096-group memory capture had prefix recent count 1 rather than 2;
that observation's cause was not established. The revised tail fixture asserts
latest count 2 across last-batch partitions of lengths 1–8, and both 4096-group
memory protocol cases now pass.

The first 4096-group RocksDB/controller attempt ended at the default 120.24-second
runtime timeout before initial capture finished. Its log is preserved at
`target/native-result-retention-tail-qualified/many-4096-rocksdb-8-controller/capture.log`;
the source task ended at 1.6 seconds and the lifetime aggregate at 55 seconds,
which is not a full capture pass. A second controller attempt used an explicit
900-second timeout in `target/native-result-retention-long-rocksdb` and failed
after 900.27 seconds with `worker runtime timed out` before any checkpoint. The
source task ended at 1.6 seconds and the lifetime aggregate at about 30 seconds;
the window, latest-result and outer aggregate tasks did not finish. Its incomplete
log is preserved at
`target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller/capture.log`.
The serial runner uses `set -e`, so the leader case did not run. The previous 120
and 900 second attempts are failures, not completed captures or performance
passes.

The historical controller invocation inside `streamr-state-build` is reproduced
here; it failed as described above:

```sh
STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=900 python3 /app/scripts/test-native-result-retention.py \
  /app/target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller \
  --scenario many --groups 4096 \
  --binary /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-df802a90a8396c4e \
  --backend rocksdb --batch 8 --mode controller \
  --checkpoint-root /app/target/native-result-checkpoints \
  --inspector /app/scripts/checkpoint_inventory.py
```

At that historical source, the 4096-group RocksDB controller and leader scopes
were pending; the 14 other scopes passed. See
[the composition record](native-result-composition.md) and
[checkpoint inventory](native-checkpoint-inventory.md).

The final native captures used the existing `arroyo_sql_testing-df802a90a8396c4e`
executable from the 606-library-test/full-workspace-build candidate; only
fixtures and documentation changed after that build. Native UUID dependent-write,
fan-out and recovery passed all eight memory/RocksDB × batch target 1/8 ×
controller/leader captures at `target/native-uuid-final-combined`. Closed-HOP
reaggregation passed the same eight-case matrix and strict value/recovery oracle
at `target/native-window-composition-final-combined`. The external native identity
query passed all eight captures; the application-owned adapter/comparator passed
16 strict initial/recovered comparisons, each with 13 unified events and two
directed merges, recorded in
`target/native-identity-final-combined/comparisons.json`. Application SQL,
oracle and comparator remain external to Streamr. These fixture results do not
establish full M3 completion, profile/session parity, packaged fault handling or
capacity qualification.

## Execution accounting

```toml
[worker.execution-resources]
memory-bytes = 16777216
max-batch-bytes = 1048576
```

This opt-in test configuration shares one DataFusion memory pool between live
stateless physical executors and projection operators. Plan decoding and execution
use the same runtime. Inputs and outputs have explicit batch limits, and reservations
remain active across collector backpressure. Output streams poll lazily without
prefetch queues. Cancellation, completion and errors release reservations and any
unused input slot. A stateless executor rejects a second input until its previous
stream completes or is dropped. Invalid configuration and budget changes while a
bounded runtime remains live fail explicitly.

The pool accounts cooperative DataFusion execution and these caller boundaries;
it is not a process RSS cap. Kernels allocate output before its batch size can be
checked, and arbitrary UDF allocations are not governed by this pool. Retaining
or cloning a yielded batch after requesting the next one requires caller
accounting. `process_single` returns a caller-owned result and propagates errors.
Generic window/join input channels and histories are not migrated by this slice.
Graph queues retain message-count bounds; byte admission for their retained batches
remains part of STR-16.
Spilling is disabled until disk capacity and cleanup are qualified; resource
exhaustion returns an error. Existing disk SQL retained-operator rejection remains.

## Durable timers and collections

`LiveTable::timers` constructs `DurableTimers` over a registered namespace.
Event and processing clocks have independent primary/deadline indexes within
that same namespace. Replacement, cancellation and related state changes can
commit in one backend batch. Snapshot scans include the exact deadline boundary,
page through arbitrarily many IDs at one timestamp, and bind cursors to the
snapshot, clock and cutoff. Before firing, the operator revalidates the current
deadline/payload against a scanned entry. A stale page cannot cancel a replacement.

`LiveTable::ranked_counts` constructs separately addressed counters and ranking
entries within one registered namespace. Entity keys use length framing; callers
choose and encode logical identity and incarnation where needed. Updating a counter and its previous/new rank is
atomic. Ranking is descending count, then ascending member bytes. This is the generic primitive's
explicit deterministic ordering contract. Counter overflow/underflow and oversized values fail. Top-K reads,
membership scans and retired-incarnation cleanup are bounded by bytes and entries.

Both views require one serial execution owner. Prepared mutations hold that
ownership through the combined state/index commit; they do not implement CAS.
Callers must admit input/related-state assembly and retain prepared-update/page
reservations for their documented lifetimes. `with_resources` validates buffer
and nested-read headroom and charges retained results to the worker state pool.
No hidden namespaces are used, so the milestone 2 full logical snapshot exporter
captures each primary/index relationship together. Schema identities must version
the operator's logical state. Restore always targets a fresh attempt.

These APIs do not schedule callbacks by themselves. Native window operators
already own watermark-driven scheduling; their state and execution still need
the selected migration/qualification work. Use native aggregate/window SQL
composition first. If actual tests demonstrate a missing timed-output behavior,
discuss the minimum generic interface before implementing it; no external
application callback path is preselected for this milestone. Any such path
would need state registration, clock/watermark/recovery ordering, admitted typed
outputs and source/sink checkpoint integration. Storage tests establish none of
those execution guarantees.

## Subsequent production completion and MAP diagnostics

The v9 memory/controller worker-loss case also passed, with exactly 6,000 raw
rows, complete aggregate CDC recovery, terminal `Finished`, 77.88 seconds of
runtime and peak combined sampled RSS of 408,879,104 bytes. Its immutable
evidence is `target/native-process-recovery-admission-v9/memory-controller/`;
owned-process cleanup completed without errors.

The v9 memory/worker case restored and completed its operator tasks, but the
worker leader cancelled its RPC server before the controller could observe
terminal status. The controller repeatedly entered recovery. The diagnosed
run was deliberately interrupted after 541.16 seconds; it did not pass, and its
last API state was `Running`. Peak combined sampled RSS was 409,522,176 bytes;
owned-process cleanup completed. Logs, checkpoint objects and interruption
evidence remain in `target/native-process-recovery-admission-v9/memory-worker/`.
The corresponding RocksDB/worker case was skipped before execution. Earlier
`leader` CLI invocations were rejected before engine startup: production uses
`worker`, whereas the SQL capture harness calls that protocol `leader`.

The independently reviewed completion repair now preserves the existing
parent/leader terminal handshake by returning before cancellation and phase
teardown on the leader path. Nonleader cancellation remains unchanged. Two
regressions exercise actual terminal-status RPC and accounted queue/listener
release. The v10 source is frozen for new integrated gates; actual production
reruns remain required. The optional
controller/catalog restart supervisor in `scripts/test-native-process-recovery.py`
has passed independent source review and syntax checks, but has not yet been
qualified by an actual controller-loss run.

The v9 plain JSON MAP-array diagnostic passed exact initial and recovered
values with peak child RSS of 179,003,392 bytes. The first SESSION/scalar-MAP
attempt produced the expected map and window values but failed its caller oracle
because unaliased projections were named `window[start]` and `window[end]`.
That attempt is preserved at `target/native-map-admission-v9/`; an explicitly
aliased query then passed exact initial and recovered values at
`target/native-map-admission-v9-aliased/`, with peak child RSS of 185,139,200
bytes. These small probes do not establish
application histogram or full profile parity.

STR-44 tracks optional per-group aggregate emission as a deferred, post-MVP
feature outside milestone 3. Existing Arroyo periodic flushing remains the MVP
behavior; the compatibility timing difference remains explicit.

## Integrated v10 completion repair checks

The frozen v10 source passed Bookworm formatting, workspace all-target checking,
strict Clippy, all 25 library suites (680 passed, zero failed, six ignored) and
the full workspace all-target build. Both new completion-lifetime regressions
passed. Exact logs, source inventory and gate results are retained in
`target/native-m3-admission-v10-build-evidence/`. The compiled diff from HEAD
`b6b2aa7a` has SHA-256
`c7c32a50ac3eb4c87e4ad84f2af8ec58de87e8cf5884dceafd4cc661273f5847`.
The SQL test binary is
`0a8c8f52ff5344e2639a26f820cfd5e32501129490d0a5cfb1baf96a84a594c1`;
the production binary is
`5c93653884a1601163a786ed0c4b1e33aa4ab31329c3a9bf898f2f91fce76abf`.

The source-free v10 memory/worker worker-loss case passed actual production
recovery: the same job moved from worker generation 1 to 2, emitted exactly
6,000 raw rows and valid aggregate CDC ending at count 6,000 and sum 17,997,000,
and reached terminal `Finished`. Runtime was 112.05 seconds; peak combined
sampled RSS was 408,834,048 bytes. Both helper and outer supervisor cleanup
completed without errors. Evidence is retained at
`target/native-process-recovery-admission-v10/memory-worker-worker-loss/`.
This verifies the previously failing completion path on the memory backend.

All six source-free v10 production cases now passed. Every case restored the
same job at generation 2, reached `Finished`, compared exactly 6,000 raw rows
and valid CDC ending at count 6,000/sum 17,997,000/min 0/max 5,999, and completed
both helper and outer supervisor cleanup without errors.

| Backend | Coordination | Injected loss | Seconds | Peak combined sampled RSS, bytes |
|---|---|---|---:|---:|
| Memory | Worker leader | Worker | 112.05 | 408,834,048 |
| RocksDB | Worker leader | Worker | 112.97 | 422,490,112 |
| Memory | Controller | Worker | 77.90 | 409,600,000 |
| RocksDB | Controller | Worker | 78.25 | 418,721,792 |
| Memory | Controller | Controller and owned workers | 77.65 | 408,088,576 |
| RocksDB | Controller | Controller and owned workers | 77.88 | 419,373,056 |

Controller-loss cases preserve catalog identity, query and program bytes, and
restore the authoritative persisted epoch with exact source counter/start time
and committed sink prefixes. Both selected epoch 1 in ready state; this does not
qualify the committing-state replay branch. Evidence, catalogs, actual objects,
logs, source-free bundle provenance and exact results remain in
`target/native-process-recovery-admission-v10/`. No owned matrix containers
remained after the terminal result.

Capacity, full application parity, write/export/publication-specific faults,
backfill and 24-hour live qualification remain open. Earlier v9 runtime results
remain evidence for their own source; this production matrix does not establish
milestone completion.

## Additional v10 SQL diagnostics

The v10 fixed-reference calendar query passed all eight memory/RocksDB ×
controller/leader × source batch 1/8 captures at
`target/native-calendar-boundary-admission-v10/`. Each checks exact committed
prefix and final endpoints, CDC continuity, and complete correlated before/after
rows. At source prefix 7 the counts `(total, c1, c7, c30, c90)` are
`(7, 0, 2, 4, 6)`; final counts are `(10, 2, 4, 6, 8)`. Date-boundary membership
and future-date exclusion use a caller-supplied fixed reference date. Peak SQL
child RSS ranged from 187,146,240 to 194,158,592 bytes. This is calendar
arithmetic and fresh-worker recovery evidence, not moving-clock expiry or an
autonomous idle-key zero.

The separate selected-append-snapshot MERGE/CASE-array wave stopped at its first
memory/controller/batch-1 attempt. Planning failed with
`state-table fusion: scalar physical output schema differs from graph edge`
before any input processing; unchanged-source failure artifacts are retained at
`target/native-existing-sql-parity-admission-v10/`. The strict schema guard stays
in place. Existing-SQL cast diagnostics are prepared but unrun; the exact schema
difference remains unobserved. No selected-snapshot or aggregate-emission parity
pass follows from that attempt.

The v10 RocksDB aggregate capacity wave passed the 8-member sanity and
10,000-member hot-key cases under controller and leader checkpoint modes.
Every pass compares full payload values, CDC before images and recovered
committed prefixes. The 10,000-member cases retained a logical payload floor of
2,559,744 bytes at the checkpoint; peak SQL child RSS was 214,138,880 bytes
(controller) and 212,725,760 bytes (leader). These cases do not reach ten times
the declared 78 MiB pool budget.

The subsequent 100,000-key, 8,192-byte-payload controller case failed before its
checkpoint: graph queue admission at `value_1` requested 258.2 KB while only
43.5 KB remained in the 16 MiB execution pool. The SQL capture failed in 0.91
seconds; the enclosing driver exited 1 with complete owned-process cleanup.
No recovery or ten-times-capacity pass follows from that case, and the matching
leader case was not run. All artifacts and unchanged-source provenance remain
in `target/native-m3-aggregate-capacity-admission-v10/`. Resource limits were
not increased to turn this refusal into a pass. Queue/backpressure diagnosis
is pending.

The v10 state-table all-key wave similarly passed the eight-key sanity under
both checkpoint modes. Its 65,000-key controller capture failed at source queue
admission after 2.28 seconds: a 257.8 KB message requested allocation with only
90.2 KB free in the shared execution pool. Its driver exited 1 with complete
owned-process cleanup, and the large leader case was not run. Artifacts remain
in `target/native-m3-state-table-capacity-admission-v10-v2/`. The intended
532,471,808-byte checkpoint floor was not reached or recovered; this is a
preserved configured admission failure, not ten-times state-table qualification.

The separate state-table queue-size-32 diagnostic subsequently passed the
unchanged 65,000-key × 8,192-byte fixture under **both controller and leader**
checkpoint modes. Artifacts are retained in
`target/native-m3-state-table-queue32-diagnostic-v10/`. Each case inserted
65,000 independent values, checkpointed the proper 64,999-row prefix, then
probed every key. Exact initial and recovered comparisons each cover 130,000
rows, including event order, action, OLD/NEW quantities and full-payload SQL
equality in OLD, NEW and same-event lookup. Recovered committed output was
exactly 64,999 rows; all 64,999 restored prefix keys were probed. The full and
checkpoint retained-payload floors were 532,480,000 and 532,471,808 bytes,
respectively, each greater than ten times the unchanged 48 MiB pool sum.

Controller peak SQL-child RSS was 216,408,064 bytes (206.38 MiB); leader was
218,284,032 bytes (208.17 MiB), both below the unchanged 512 MiB ceiling.
Enclosing drivers exited 0 in 528.67 and 537.48 seconds, within their
1,800-second SQL deadlines, with complete owned-process cleanup and stable
final provenance. The supplied existing configuration was
`ARROYO__WORKER__QUEUE_SIZE=32`, replacing the default 8,192-row queue;
no execution/state budget, cardinality or oracle was relaxed. Invocation and
pinned config-loader/test-configuration evidence establish that binding; no
separate numeric effective-queue scrape was taken. The earlier default-queue
admission failure remains preserved and is not a pass.

Independent evidence inspection rechecked every initial/recovered row and the
retained checkpoint inventories: controller 1,455 objects / 283,159,385 bytes;
leader 1,454 objects / 283,195,508 bytes. Each contains 1,445 Parquet objects
summing to 282,758,172 bytes. Every inventory path, size and SHA-256 matched.
These compressed physical sizes are not the logical retained-payload floor;
no separate raw-IPC checkpoint-content decode was performed. Recovery plus
all-key full-payload equality is the value oracle. Frozen v10 source/build
proof hashes are `4d1782df…` / `481a00ed…`, SQL ELF `0a8c8f52…`, driver
`24580ca9…` and launcher `c258d45a…`; all five v10 gates passed. Input, expected
rows, query, capture log and outputs are independently hash-pinned per case.
This qualifies that frozen binary and configuration, not later source repairs.
It is singleton same-process fresh-worker epoch-1 recovery, not an OS-process
loss, greater-than-host-RAM, multiple-epoch or full M3 readiness claim. See
[state-table capacity evidence](state-table-all-key-capacity.md).

The separate v10 aggregate queue-size-32 diagnostic kept all 100,000 keys,
8,192-byte values, 300,000 inputs, checkpoint prefix 199,999, the 78 MiB pool
budget and the 512 MiB SQL-child ceiling. The actual SQL child supplied
`ARROYO__WORKER__QUEUE_SIZE=32`; a process identity/environment witness is
retained beside the case. Effective configuration follows the pinned config
loader; no separate numeric configuration scrape was taken. The initial phase
reported 100,000 rows after approximately 27 minutes, then the SQL capture
exceeded its declared 1,800-second deadline during checkpoint/recovery. The
driver exited 1 with complete owned-process cleanup and unchanged final
provenance; the leader case did not run. Exact-value comparison and ten-times
recovery qualification did not complete. Artifacts remain in
`target/native-m3-aggregate-many-queue32-diagnostic-v10/`.

The existing-SQL whole-CASE `CAST(... AS TEXT[])` selected-snapshot diagnostic
also failed at the same strict planner schema guard before input processing.
The nine input records, six final rows and three checkpoint rows were unchanged.
The SQL child exited 101 after 0.20 seconds, with 148,058,112-byte peak RSS and
complete cleanup. Post-run pins remained unchanged. The original failure is
retained separately under the fresh
`target/native-selected-cast-result-admission-v10/` evidence. Actual physical
and declared schemas still need observation before a repair; no guard bypass or
new SQL syntax was introduced.

Repeated physical read-view setup during aggregate result draining is a
source-demonstrated integration cost. The proposed lifecycle decision is in
[the RocksDB read-view proposal](rocksdb-read-view-proposal.md), awaiting user
agreement. No native-view ownership dependency or contract change has been
implemented.

## Integrated v11b bounded-scope repairs

After the frozen-v10 state-table capacity wave completed, two reviewed
contract-preserving optimizations were integrated: memory scans stop at an
exhausted logical prefix/exclusive end, and indexed aggregate chunks containing
only physical appends use the existing point-read scope. Indexed retractions
retain stable snapshot scopes. SQL results, admission limits and durable
checkpoint machinery are unchanged. The planner's strict scalar schema check
also now prints both schemas when rejecting a mismatch; the check remains intact.

Fresh Bookworm formatting, workspace all-target checking, strict Clippy,
workspace library tests and all-target build passed serially in the existing
build target. The library run contains 25 suites, 683 passed tests, zero failures
and six explicitly ignored dedicated-runtime tests. The new real-backend scan
visit bound, indexed append snapshot avoidance, keyed expiry/filter/null
assertions and existing retraction cancellation checks all passed.

Evidence is at `target/native-m3-admission-v11b-build-evidence/`. Frozen source
SHA-256 is `44c5106faf34339730b9417d1f8efc5702c24b751066b1525c90375f1750a2a2`;
completed build-evidence SHA-256 is
`b2aabc80c859e60a2831c2e4100a4925deb6c5845d9f39c787ad0c484e9164e5`.
SQL ELF SHA-256 is
`3adf4ee594227af25b44d4ab82ba55bd25dda947e010aa2fa780ea68446225c7`;
production ELF is
`d6b3e70644714176c7d05521f0dd1bf9bf4be9f025b06f25f72d0dbc2648a099`.
The initial v11 formatting-only failure is preserved separately; its argument
line wrapping was corrected before freezing v11b. These combined checks do not
requalify the earlier-source capacity/recovery runs or establish a measured
speedup. RocksDB read-view lifecycle changes remain proposed, unimplemented and
subject to explicit agreement.

The unchanged-guard selected-snapshot cast diagnostic then ran against that
qualified v11b ELF and failed before processing input. Evidence is at
`target/native-selected-cast-result-admission-v11b/`: SQL exit 101, measured
peak RSS 147,075,072 bytes, child duration 0.206 seconds, cleanup complete and
post-run provenance unchanged. The detailed rejection establishes that the only
schema difference is the outer `changed_fields` field's nullability: physical
`false`, declared `true`. Both are the same List of nullable Utf8 elements named
`field`; all other fields, names, types and nullability match. This is evidence
for investigating safe nullability widening in the fusion adapter, not proof of
native selected-snapshot parity. The strict guard remains enabled, and no type
coercion or unsafe nullability tightening is authorized by this result.

## Reproduction

Use the prescribed Bookworm image and serialized build queue from
[the build procedures](../.claude/build-test.md). Preserve the shared Cargo
target. The first-slice verification command is:

```sh
bash scripts/verify-milestone3.sh
```

The script needs no application checkout. It runs state/RPC/worker units,
SQL-testing compilation, all-target checks, strict Clippy, formatting and diff
checks in the prescribed development container.

External applications may use the opt-in `external_sql_checkpoint_capture` test
with an externally prepared single-file SQL query. The harness accepts absolute
query/output paths and explicit expectations; it has no business schema or oracle.
Set `STREAMR_CAPTURE_QUERY`, `STREAMR_CAPTURE_OUTPUT`,
`STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT`, `STREAMR_CAPTURE_EXPECTED_ROWS`,
`STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS`, and `STREAMR_CAPTURE_CHECKPOINT_EPOCH`.
Optional `STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS` declares the uninterrupted
capture count separately; it defaults to `STREAMR_CAPTURE_EXPECTED_ROWS`.
Updating aggregates can emit additional intermediate changelog rows when a
checkpoint forces a flush. Compare their complete values and final materialized
result as well as the explicitly declared counts.
Use `STREAMR_TEST_BACKEND=memory|rocksdb`,
`STREAMR_TEST_CHECKPOINT_MODE=controller|leader`, and optional
`STREAMR_TEST_EXECUTION_BYTES` for the engine configuration. Run it alone:

```sh
cargo test --locked -j4 -p arroyo-sql-testing external_sql_checkpoint_capture \
  -- --ignored --test-threads=1 --nocapture
```

The source must be a single control-waiting file source; the graph must have
parallelism one. Expected output counts are independent of input counts, so
filters and multiple state owners are not assumed to preserve rows one-for-one.
The initial capture uses `.initial.jsonl`; the recovered capture uses the supplied
output path. The harness recreates the program from the selected published
checkpoint and source/sink metadata. Worker cancellation is not a process-kill,
Kafka, remote-storage or beyond-RAM proof. Fixture preparation, application output
schemas, reference normalization and business comparisons belong externally.

Earlier compatibility evaluation used a pinned external Arcstream fixture at
`10f779469748d0c0dde6d5af3aa7375f8dbc36d3`. Its six initial/recovered identity
comparisons passed at Streamr `a2d2aba7` with 16 MiB execution accounting. That is
historical external evaluation evidence, not an engine dependency or profile/session
readiness claim. The timer and collection tests separately exercise the generic
production logical exporter and fresh RocksDB restoration.

## Native updating timestamp and typed-array repair (2026-10-05)

All five Bookworm gates passed on the repaired worker and planner, with 636
library tests (four ignored). The SQL capture executable SHA-256 is
`35dafbefefc91699a486e686615dbbdc580888e6ff4764ad101a5db75471fa2f`.
The compiler-source hashes and exact gate references are recorded in each
proof directory's `source-evidence.json`. Combined results are at
`/tmp/streamr-m3-timestamp-identities-union-fixed-v3-pipeline-results.json`.

| Selected proof | Captures and strict comparisons | Artifact |
| --- | --- | --- |
| Current-member hidden timestamp MAX; COUNT and caller MAX; direct/duplicate UNION/shared CTE; updates, group moves, deletions | 24 | `target/native-updating-timestamp-identities-union-fixed/comparisons.json` |
| Ordered TEXT and flat STRUCT ARRAY_AGG; NULL, duplicates, FILTER, retraction and deleted-group removal | 8 | `target/native-updating-array-cdc-timestamp-identities-union-fixed/comparisons.json` |
| Standard COALESCE for empty filtered TEXT/STRUCT arrays | 8 | `target/native-array-empty-coalesce-timestamp-identities-union-fixed/comparisons.json` |
| External already-ranked TEXT array assembly | 8 | `target/native-profile-ranked-array-timestamp-identities-union-fixed/comparisons.json` |
| External 14-field profile core | 8 | `target/native-profile-core-timestamp-identities-union-fixed/*/comparison.json` |

All matrices span configured memory/RocksDB, controller/leader checkpoint
ownership and source batches 1/8. They compare uninterrupted final, committed
checkpoint and fresh-worker recovered values, including exact CDC before-images.
A separate 64-KiB member against the 32-KiB native aggregate value limit failed
with exit 101 and the expected collection budget diagnostic; it is a negative
resource test and is excluded from the 56 successful captures.

The [regression investigation](updating-aggregate-timestamp-regression.md)
records the upstream sliding-MAX limitation, Streamr's exact-retraction failure,
the approved row-ID index repair and the separate UNION graph consolidation.
The [generic array fixture](native-updating-array-cdc.md) retains the earlier
constructor, schema and zero-count failures and records the final qualified
result. Caller aggregate semantics and configured collection limits are
unchanged. Native state codec fingerprints reject incompatible old checkpoints;
recompiled/reordered-program migration is not qualified.

These results do not qualify lifetime ROW_NUMBER ranking, general nested
collections, hot/unrestricted collection capacity, full profile/session output,
all timing rules or complete milestone 3 readiness. External schemas and oracles
remain application-owned.

## Existing-SQL updating top-five array (2026-10-05)

The [generic 23-event fixture](native-updating-top5-sql.md) uses existing SQL:
keyed `COUNT(*)` per item, ordered typed-Struct `ARRAY_AGG` of the changing
counts, then `array_slice(..., 1, 5)`. All eight memory/RocksDB ×
controller/leader × batch-1/8 captures passed strict typed CDC, the 21-event
committed checkpoint and fresh-worker final-value comparisons on source
`8649bf440fd55e4cf4289b2bbc72969348b821f7` and executable SHA-256
`35dafbefefc91699a486e686615dbbdc580888e6ff4764ad101a5db75471fa2f`.
The exact results, source inventory and per-case manifests are at
`target/native-updating-top5-existing-sql/{comparisons.json,source-evidence.json,manifest.json}`.
The checkpoint `c` array is `f:2,g:2,a:1,b:1,c:1`; the recovered final array is
`a:1,b:1,c:1,d:1,e:1`, while `b` remains `y:1`. CDC before images and every
emitted typed array were checked against forward source-prefix values. The
small fixture's intermediate comparison assumes each item's count becomes
visible in source-prefix order; it is not a general cross-group atomicity test.

This result bounds only the output array. The aggregate still retains all
distinct item counts and materializes its complete ordered member array before
slicing, under existing collection limits. It does not qualify bounded top-K
state, high-cardinality/hot-key capacity or SQL `ROW_NUMBER` output rows.

## Existing SQL clock diagnostic (2026-10-05)

`target/native-clock-probe/runtime.log` records a planner rejection for
`CURRENT_TIMESTAMP`: `Invalid function 'current_timestamp'`. A separate
`NOW()`-only projection at `target/native-clock-probe-now-only/comparison.json`
passed initial, one-row checkpoint and three-row recovered value comparison
with real pre-EOF idle holds of 5,001 and 5,000 ms. Its two `NOW()` columns
were identical and stayed at `2026-10-05T02:36:28.071163175` across initial
and recovered phases, while the source event timestamps varied by one second.
This shows a fixed query-planning value in this tested path, not a dynamic
emission clock or timer. Both probes used the same `8649bf44` source and
executable SHA-256 above. No clock semantics were changed.

## Native Parquet checkpoint correction (2026-10-05)

Source `4fbd740a0861dddf1bbdd9e264f2e6fd11742413` replaces the live-state
binary page exporter with a bounded adapter to Arroyo's existing keyed-state
Parquet schema/writer/reader. Existing checkpoint coordination and ownership
remain; legacy binary reads are retained. See
[the correction record](native-checkpoint-parquet-adapter.md).

All five Bookworm gates passed with 645 library tests (four ignored). Exact
initial/checkpoint/fresh-worker comparisons passed 60 generic captures: 24
updating timestamp/count/MAX, eight typed arrays, eight existing-SQL top-five,
eight typed state-table MERGE/lookup and twelve TUMBLE/HOP/SESSION cases.
Four RocksDB checkpoint fault tests passed across controller and leader modes.
The executable SHA-256 is
`1c57540e44ff31ba736a9ac037212fad8628729adaa6841e5e0dd1c954f980ac`.
Source inventories, gate logs, complete pipeline results and independent review
are retained in `target/native-hop-capacity-parquet-v3-65000/build-evidence/`.
The failed first two qualification attempts are separately archived; their
results are not substituted for the final source.

Both 65,000-key RocksDB HOP protocols passed exact comparisons of every one of
130,000 initial and recovered item/window outputs and full 8,192-byte payloads.
Each retained checkpoint has a conservative 532,471,808-byte payload floor above
ten times the declared 50 MiB pool sum. Controller peak whole-child RSS was
356,876,288 bytes; leader peak was 352,108,544 bytes, both below 512 MiB.
Each published window inventory has 1,476 Parquet files, with all references,
sizes and SHA-256 checksums verified. Operator metadata is 304,062 bytes for
controller and 339,486 bytes for leader, under the unchanged 3 MiB cap.
The leader's complete published manifest is 340,747 bytes. Results are at
`target/native-hop-capacity-parquet-v3-65000/{measurements.json,checkpoint-measurements.json,source-evidence.json}`.
The combined pipeline exited zero with frozen Rust source hashes unchanged;
all seven CI checks passed on `4fbd740a`. This closes the observed selected HOP
leader capacity failure. Broader window/quiet-key behavior, profile/session
parity, timers/emission, backpressure, backfill and live/fault gates remain open.
Milestone 3 is incomplete.

## Shared execution admission qualification in progress (2026-10-05)

The current staged candidate shares execution admission across graph queues,
network transfers and updating-aggregate expression/output lifetimes. Independent
source review has approved these changes; runtime qualification is pending.
The fresh full-workspace attempts are recorded separately and must not be
combined into an overall pass:

- Attempt v1 passed formatting and failed compilation on private module
  visibility. A one-line crate-local visibility repair was independently reviewed.
- Attempt v2 passed formatting, checking and strict Clippy. Library tests found
  an incorrect escaped-key test budget (29 encoded bytes required, 28 allowed).
  The test now computes the maximum actual row cost; production scan code and
  exact row/snapshot assertions were preserved.
- Attempt v3 passed formatting, checking and strict Clippy. Library tests exposed
  a real receiver-drop race: Tokio can admit a message before receiver teardown
  and publish it after the receiver drains, retaining Arrow data while a sender
  remains alive. A private synchronous publication/close-and-drain lock repairs
  both queue modes. Independent review approved the fix, its metadata accounting
  and deterministic ownership assertions. No asynchronous wait holds the lock.

- Attempt v4 passed formatting, checking and strict Clippy. Across 24 completed
  library suites, 669 tests passed, one failed and four were ignored. The receiver
  ownership tests passed. The new aggregate cancellation fixture failed before
  its intended wait: two held state scopes exceed its four-MiB write pool because
  each scope reserves five times its configured write bytes plus operation
  overhead. The reviewed test-only repair preserves the real snapshot suspension and
  cleanup assertions; production admission limits remain unchanged.

Attempt v5 passed all five required Bookworm gates against frozen reviewed
source: formatting, whole-workspace checking, strict Clippy, 670 library tests
with four explicitly ignored, and the full all-target build. It reused the same
container/cache and machine build queue. Logs and source
hash inventories are preserved under `/tmp/streamr-m3-admission-v{1,2,3,4,5}-*`.
The first four attempts failed; none establishes acceptance for this candidate.
Fresh native runtime, resource and recovery qualification is pending after this
successful combined build. Frozen source, gate logs and executable hashes are
retained in target/native-m3-admission-v5-build-evidence/. Prior baseline captures remain evidence for their recorded revisions.

The fresh v5 small runtime wave has now finished. Its 31 qualification steps
passed, including preparation, eight typed-result composition captures, ordered,
closed, many-key and hot-key SESSION captures across both backends and both
checkpoint protocols, two state-table captures, and four continuous SESSION
captures. The continuous fixture crosses 24 hours of event time; it does not
establish a 24-hour live run or larger-than-memory acceptance. Scheduled CDC
endpoint assertions and the existing NOW query-start clock diagnostic also passed.
All results and executable/source hashes are retained in
`target/native-m3-admission-v5-small-runtime-v2/`.

The wave exited nonzero because two capability diagnostics failed: plain JSON
MAP output and a scalar MAP assembled after SESSION aggregation panic while
constructing a serialization schema. These failures remain open; neither is
full session histogram or application parity evidence. The scheduled timing
observations establish periodic aggregate flushing, not immediate per-key
creation or first-pending-change debounce.

Source-free production process-loss attempts are recorded separately in
`target/native-process-recovery-admission-v5/`. The first attempt could not start
a worker with non-loopback defaults in a network-isolated container; its owned
process was stopped and cleanup recorded. The second uses the existing loopback
address configuration but exited before qualification completed. Neither attempt
establishes production recovery; their failure evidence is preserved.

The third production attempt accepted the fixture configuration but timed out
after 90.47 seconds in Scheduling, with empty outputs and completed cleanup.
Tracing showed an introduced lifetime regression: worker initialization dropped
its returned RunningEngine after extracting controls. Its new destructor aborted
the network listener; dropping the listener's shutdown guard cancelled the
worker, including its control receiver. A reviewed repair retains RunningEngine
inside the existing waiting/running phase state, with a real listener/control
lifetime regression test. This repair is integrated but not yet build- or
runtime-qualified. The failure remains under
`target/native-process-recovery-admission-v5/rocks-controller-v3/`.

Four small many-key aggregate captures also passed on the frozen v5 candidate,
with exact retained MAX payloads, COUNT updates for every key, changelog
before-images and committed-prefix recovery assertions. Results are at
`target/native-aggregate-admission-v5-many-small/comparisons.json`; eight keys
and 64-byte payloads do not qualify larger-than-RAM capacity. The separate
1024-member ARRAY_AGG-then-slice probe reproduced the expected precise collection
budget rejection at peak child RSS 192,860,160 bytes. Its test exited 101 and its
diagnostic supervisor exited 2; this is a validated limit refusal, not successful
ranking qualification. Artifacts are at
`target/native-top5-admission-v5-negative/`.

The new candidate also integrates the reviewed unused-schema serialization
repair, shared execution-pool metrics and checkpoint-byte HELP correction. The
metrics observe the latest configured pool through weak references and do not
replace its admission mechanism. Existing checkpoint byte counter values are
preserved: they measure successfully transferred immutable object bytes, not
logical payload bytes. All these changes require a fresh combined build and
runtime qualification; v5 gate success applies to the preceding frozen source.

The next combined attempts are recorded independently: v6 passed formatting but
failed compilation on two new fault-harness loops; using the existing
OperatorChain iterator repairs those loops without changing their assertions.
V7 passed formatting and compilation but failed strict Clippy on a redundant
return binding in the same test helper. The independently reviewed direct-return
repair passed v8 formatting, checking and strict Clippy. V8 library tests then
found an incorrect assumption in the newly added Avro initialization test:
the existing container writer appends without flushing a small pending block.
The original serializer has the same behavior; lazy schema construction did not
introduce it. The reviewed test-only correction uses existing raw datums and
checks exact decoding, full byte consumption and repeated schema initialization.
No production Avro writer behavior changed, and container correctness is not
qualified. V9 passed all five combined Bookworm gates: formatting, checking,
strict Clippy, 678 library tests across 25 suites (six opt-in tests ignored),
and the full all-target build. Frozen source, logs and executable hashes are
retained in `target/native-m3-admission-v9-build-evidence/`. The SQL test binary
SHA-256 is `83f2a024a20d40ee01aa9a79ceca47e816c372089c47ce5fe8c9522fd5660e53`;
the production binary is
`95b3ca9006e5eb18b207aa2bd45936d60e3c3934f96d33611b1eda7e6786285b`.
Both belong to the frozen working-tree changes on base `b6b2aa7a`, not the
unmodified committed base.

The fresh source-free v9 production RocksDB/controller worker-loss case passed
in 78.99 seconds. It reached Running, retained and independently decoded epoch
1 with source counter 795 and exact committed sink prefixes, then killed the
actual generation-1 worker while its controller remained alive. A different
worker recovered the same job in generation 2. All 6,000 ordered raw rows and
69 aggregate CDC transitions passed, ending at COUNT 6,000, SUM 17,997,000,
MIN 0 and MAX 5,999. Combined sampled controller/worker peak RSS was
424,820,736 bytes; owned process cleanup completed. The top-level process was
forcibly terminated during bounded cleanup after Finished, not restarted as
part of this worker-loss proof. Evidence is retained in
`target/native-process-recovery-admission-v9/rocks-controller/`, with the
immutable source-free bundle and source/build hashes alongside it. Other
backend/protocol combinations, whole-controller restart and larger/live fault
qualification remain pending.

All eight fresh v9 native checkpoint-fault cases passed: updating aggregate and
SESSION owners, controller and leader protocols, failed epoch-2 export followed
by selected epoch-1 recovery, and retained epoch-2 recovery after cleanup. Each
uses independent exact typed output and committed-prefix assertions, real
native checkpoint objects and preserved owned runtime artifacts. Child peak
RSS ranged from 195,510,272 to 196,902,912 bytes, below the declared 512-MiB
envelope. Evidence is retained in
`target/native-m3-admission-v9-native-fault-wave/`, including frozen source,
binary, caller fixture and launcher hashes. These are small local export and
retention faults, not remote publication, production controller loss, capacity
or 24-hour qualification.
Failed attempts do not
establish build acceptance; logs and complete source hash inventories are
preserved in `target/native-m3-admission-failed-build-evidence/` and under
`/tmp/streamr-m3-admission-v{6,7,8}-*`.

A read-only retained-checkpoint measurement passed for both v5 RocksDB SESSION
checkpoint protocols. It decodes the actual Parquet transport and raw Arrow IPC,
then compares seven caller-projected rows with an independent oracle derived
from the original synthetic input prefix. Both contain exactly seven rows and
448 attribute UTF8 bytes, excluding keys, schemas, indices and copies. Deliberate
wrong payload, missing row, duplicate identity, wrong type and one-nanosecond
timestamp changes were all rejected. Evidence and pinned helpers are retained
in `target/native-m3-retained-ipc-small-v1/`. This small measurement validates
the measurement path; it does not qualify large state, live RSS or production
recovery.

The user deferred the optional immediate-first/per-group first-pending aggregate
emission policy until after MVP, tracked in STR-44. Milestone 3 uses Arroyo's
existing periodic flushing and records the observed timing difference in
compatibility evidence. STR-44 is related post-MVP work, not a milestone 3 or
MVP blocker; no new emission policy has been implemented.

## Validation status

Frozen v12b passed all five combined Bookworm gates: formatting, workspace
all-target checking, strict Clippy, 694 library tests across 25 suites (six
opt-in tests ignored), and workspace all-target build. Evidence is retained in
`target/native-m3-admission-v12b-build-evidence/`. Its source inventory SHA-256 is
`bcb48d234ce81955b0ed287f7bfb571716ffe2a86da58ac525e6d360fff4a7ac`;
build evidence SHA-256 is
`114b55d62cb0eb97e35e9d10fccd2b22c60ef3feba80e5b90b3e3bfe953b7f70`.
The SQL test executable SHA-256 is
`b97b63e775ee6c4be290d7e003f7bced6890000eaf5a9ab69365a523a1114317`;
the production executable is
`75ea281ac6ad53174920bf097f88ef881240f19c5505aca1077f1d3459e65869`.
Both original and explicitly cast selected-snapshot CASE planner regressions
passed. This qualifies the narrow outer-nullability schema repair and native
MERGE fault-harness support at the unit/build level. The preceding v12
strict-Clippy failure was test-only and is retained separately.

All sixteen v12b selected-snapshot captures passed: original uncast SQL and its
CASE-result cast variant, each under memory/RocksDB, controller/leader, and
source batch sizes 1/8. Every case compared all six initial and recovered rows
and the three-row committed prefix with the independent typed oracle, retaining
actual owner/configuration, epoch-1 metadata and checkpoint objects. Child peak
RSS ranged from 185,303,040 to 190,922,752 bytes, below 512 MiB; all child cleanup
completed and final source/build/binary/helper inventories remained unchanged.
Artifacts are in `target/native-selected-snapshot-parity-admission-v12b/`.
This establishes selected append-snapshot SQL and prior-snapshot values, not
the timing or contents of actual aggregate emissions, full profile parity or
production process-loss recovery. Native MERGE fault checks subsequently passed
on v13 as recorded below.

The integrated source review found two delivery regressions still present in
v12b: unconditional flushing on Immediate source completion, and an ignored
buffered delivery error allowing a later small checkpoint barrier after lost
rows. Those repairs were independently reviewed and applied in v13, below.
Consequently these passing
build gates do not establish milestone readiness or source/checkpoint delivery
acceptance. The repair must preserve the original conditional completion and
checkpoint coordination contracts and retain the first typed failure.

The independently reviewed three-file repair was subsequently applied as v13.
Immediate completion now checks the first delivery failure without flushing;
Graceful/Final retain their existing flush-and-signal path. A failed buffered
delivery reports and retains its original typed failure before returning, so
ignored errors cannot forward a later barrier or report successful completion.
Borrowed/owned failure conversion shares the existing classification logic.
Real operator/deserializer/admission/checkpoint regression tests accompany the
repair. V13 source inventory SHA-256 is
`13abe6bd5c054d2db0d5491644215608a9ddac700abb2d94f0ed0195514e3dee`;
all five fresh combined Bookworm gates passed, including 698 library tests
across 25 suites (six ignored) and all four new source-delivery/error-conversion
regressions. Build evidence is retained in
`target/native-m3-admission-v13-build-evidence/`, SHA-256
`abf4a10fac2ced4e354dcf7299580615e1bcdcfb09cc243223cb43eea7784fb3`.
SQL executable SHA-256 is
`2ba930686719a43798db1e0e90d8a063e7e50c2a58795f884638d73bb30d4ebf`;
production executable is
`cd1d8e150315e278097e55571a3f13e04c8a6d2690df20f96db127aa42765889`.
No checkpoint format, writer or coordination protocol was replaced.

All four fresh v13 RocksDB native MERGE fault cases passed under controller and
leader checkpoints: failed epoch-2 export without publication followed by
selected epoch-1 restore, and selected epoch-2 restore after epoch-1 cleanup and
two recovery attempts. Every case checks all eleven ordered typed output rows,
including mutation OLD/NEW and same-event lookup values, against independent
oracles. Checkpoint source prefixes 3/6 match the persisted sink offsets 254/507
bytes and exact committed outputs. Retained native typed checkpoint objects,
rendered SQL, input/oracle files and complete artifact hashes are preserved in
`target/native-str43-merge-fault-admission-v13/`. Child peak RSS ranged from
187,502,592 to 188,985,344 bytes; every child cleanup completed and final
provenance remained unchanged. These small same-process export/retention tests
do not qualify OS process loss, large-state capacity,
the forty-case native output comparison or full application readiness.

The v13 forty-case native SQL wave stopped after eight passing
operation-fixture captures (memory/RocksDB, controller/leader, batch targets
1/8). Those eight checked all 161 initial/recovered rows and the 80-row committed
prefix against the independent declared golden. The first shared-CTE case failed
planning with `state-table fusion: scalar function concat has unqualified purity
or allocation bounds`; SQL exited 101 after 0.20 seconds, with child peak RSS
144,109,568 bytes and complete cleanup. Final frozen inventories remained
unchanged. Evidence is retained in `target/native-str43-parity-admission-v13/`.
This is a demonstrated limitation of Streamr's fusion admission for an existing
SQL function, not justification for new syntax or relaxed memory checks. That
revision did not complete the five-fixture comparison; fresh v14b results are
recorded below. Full milestone qualification remains open.

The source-free v13 production matrix subsequently passed all six process-loss
cases: memory/RocksDB worker loss under the worker and controller checkpoint
protocols, and memory/RocksDB whole-controller loss under the controller
protocol. Each case compared the complete raw sequence 0..5999, the committed
source/sink prefixes and typed aggregate changelog, finishing with count 6000,
sum 17,997,000, minimum 0 and maximum 5999. The selected checkpoint was epoch 1;
replacement workers used generation 2, and whole-controller recovery retained
the same catalog/job and advanced its run. Sampled combined controller/worker
RSS ranged from 406,224,896 to 428,040,192 bytes under the declared 1 GiB limit.
Every inner and outer cleanup completed. Evidence is retained in
`target/native-process-recovery-admission-v13/matrix-results.json`, SHA-256
`64ba2d45724d9961120bc2f309efc6f4aac864cedff7be4d9ccc8b55934efb8e`;
immutable bundle provenance SHA-256 is
`5aa0a5ecd61a67efb850da7f451dde6e4b50026133dd537c6f812cd27276ca9f`.
These finite impulse/single-file checks establish the recorded restart paths,
not broker delivery, export/publication-phase process loss, large-state capacity,
complete external profile/session parity or a 24-hour live qualification.

A separate quiet-target/active-peer standard-SQL diagnostic remains a failure.
Its HOP output followed by ordered LAST_VALUE restores the selected source-4
prefix exactly to lifetime 3/recent 2, but both eight-second live holds finish
at lifetime 3/recent 1, rather than the required recent 0. Real peer-key events
advance the declared source clock; no fake target event was supplied. The
source-position hold and peer-clock projection do not directly measure the
runtime watermark, and this does not qualify an all-source-idle clock.
The original launcher reported 882,495,488-byte child peak RSS after hashing
the entire executable with read_bytes. A reviewed hash-only retry using
streaming file_digest reports 192,520,192 bytes with the same SQL, oracle and
512 MiB limit; it still fails the zero-value assertion. Both failures are
preserved in `target/native-quiet-target-active-peer-admission-v13/` and
`target/native-quiet-target-active-peer-streaming-admission-v13/`; SQL children
exit 0, cleanup completes and final pinned inventories remain unchanged.
This confirms the selected latest-nonempty-window query does not satisfy the
expiry requirement; it does not by itself justify changing window semantics.

All four v13 small RocksDB SESSION raw-checkpoint captures passed under
controller/leader protocols. The hot fixture retains seven open input rows
and 448 UTF8 payload bytes, then emits exactly one initial/recovered session;
the many fixture retains fifteen rows and 960 bytes, then emits exactly eight
sessions. Both selected checkpoint outputs are empty. Independent raw Arrow
row comparison verifies projected fields, identities, NULLs, exact timestamp
nanoseconds and full payloads, in addition to final SQL outputs. Child peak RSS
ranged from 195,219,456 to 196,399,104 bytes, below 512 MiB. Evidence is retained
in `target/native-m3-session-small-raw-ipc-admission-v13/wave-results.json`,
SHA-256 `1e3a09290bf444932f906494c1678ab1c47de4b9e727f8d2d3cfa663c3286211`.
These are small fresh-worker captures, not >=10x capacity, production process
loss, complete external session parity or the 24-hour live gate.

The independently reviewed stock UTF8 CONCAT repair guards newly admitted
fused Projection/Value expressions before string expansion, with one cumulative
backing-byte allowance shared by nested calls and released at the operation
boundary. Stock NULL/value semantics, plan serialization and configured limits
remain unchanged. Existing direct state-access expression accounting is
unchanged; this is not a universal allocator ledger or a per-input transaction.

The first v14 qualification stopped after formatting passed: all-target checking
failed with E0507 in the new zero-row array comparison test, before Clippy,
units or the build ran. The failure is preserved in
`/tmp/streamr-m3-admission-v14-check.log` and the corresponding results JSON.
The independently reviewed test-only correction compares borrowed array
references without changing the zero-row oracle or production code.

Fresh frozen v14b then passed all five combined Bookworm gates: formatting,
workspace all-target checking, strict Clippy, 706 library tests across 25 suites
(six opt-in tests ignored), and workspace all-target build. All eight new CONCAT
admission, allocation, value/schema and refusal/retry regressions passed.
Evidence is retained in `target/native-m3-admission-v14b-build-evidence/`.
Source inventory SHA-256 is
`d16b92b6ec3b44c741bd91bd5acc4762baaea594f94c6a3e1f372ee6b0040ca3`;
build evidence SHA-256 is
`8791ffe6eb08f851463ab792095ae898fbe10f71dfd8a88b79daa3389f4f79fa`.
SQL executable SHA-256 is
`7e1fb8034285ead7e0bae311817fc0426ea095bfb6fd198dd122b2420387992b`;
production executable is
`bf496d076f8db9dc518e603a4ded61eb9c18bc81c08f281342f4304781f3fe8e`.
These gates qualify the repair at the unit/build level. The fresh v14b native
state-table matrix subsequently passed all forty captures: five original fixture
streams, each at source batch targets 1/8, memory/RocksDB and controller/leader.
Every case independently compared complete initial and recovered ordered rows
with the independent declared golden, including exact JSON field sets, types, NULLs,
Booleans and values. Operations/shared-CTE cases checked 161 rows and an 80-row
committed prefix; sequential/computed/filter cases checked five rows and a
two-row committed prefix after three input rows. Every capture reported epoch 1,
one singleton FusedStateTable owner, its declared
backend/protocol and the unchanged 16 MiB execution pool. Checkpoint job/lineage
paths and all 436 retained object hashes were checked. Child peak RSS ranged
from 182,763,520 to 191,946,752 bytes, below 512 MiB; every child cleanup completed
and final source/build/ELF/helper/fixture inventories remained stable.

Evidence is retained in `target/native-str43-parity-admission-v14b/`.
Terminal result SHA-256 is
`082da414721cd3ef83e01efcaabdb19d3c878e4fddae2ab7f990559a47ee5f69`;
forty-case results SHA-256 is
`2d8d00989be791392265871b44bbc4e7380f74677f166fbcf4661e6b2d6cdc11`;
provenance SHA-256 is
`9311ee954c8e06d64a46cfdfb5ddc9b51c83c240b730ae96f4e9d2ecd5604b66`.
The v13 eight-pass/shared-CTE-failure evidence remains unchanged. This qualifies
these five finite native SQL streams and fresh-worker native-checkpoint recovery,
not full application parity, live/capacity acceptance, production process loss,
full milestone readiness. Export/
retention faults and production restart paths have separate source-pinned evidence;
this value matrix does not replace those checks. No passing matrix or build
establishes milestone readiness.

Qualification of `a2d2aba7` passed 153 Bookworm units (33 RPC, 56 state, 64
worker), 25 isolated native fixture runs, all-target checks and strict Clippy.
CI passed 468 library tests and 12 integration tests. Validation for the current
application-boundary correction is recorded against the exact PR head in
[PR #6](https://github.com/jasadams/streamr/pull/6); no pending check counts as
acceptance.

STR-16, STR-29 and the STR-28 inventory remain in progress. STR-17, STR-20, STR-26
and STR-32 retain their full acceptance gates. Milestone 3 requires reviewed
foundation/map artifacts, profile/session parity, actual hot-key and >=10x state
budget qualification, representative backfill and a 24-hour live/fault run before
readiness. No release, deployment or canonical-topic change is part of this branch.
