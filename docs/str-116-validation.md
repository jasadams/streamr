# STR-116 obsolete-generation cleanup

## Source and scope

Task branch `jason/str-116-cleanup-20261015` starts at fetched default branch
`f09434d38a0f9b0a78dc8bc4fd019738283d2a1f`. Guidance read: `AGENTS.md`,
`.claude/build-test.md`, `.claude/team.md`, `docs/milestone-3-status.md`,
`docs/milestone-3-work-plan.md`, `docs/build-qa-troubleshooting.md`, and the
current STR-116 description. Current user instructions remove old automatic
deadlines and review signing. STR-115 TTL expiry and STR-112 encoding/checkpoint
production sections remain outside this change.

The native generation cleanup queue is a fork addition. Pinned upstream-era
`74c967e343bfa0d0741578c01aeb356edb613980` aggregates expire their in-memory
cache during flush after updates; they have no C-marker/native member reclamation
loop. This change preserves the fork's existing M/R/B/J/Q/U/T state machine,
serial ownership, generation isolation, calendar due ownership, tombstone
retraction ordering and atomic admitted writes. The previous source audit is
`/home/jason/qa-evidence/str113-session-drain-20261014/rocksdb-scan-audit.md`.

## Finite measurement plan

Before production edits, executed two existing-test-harness diagnostics:

- Native RocksDB mechanism: 24 groups ×3 obsolete generations ×24 entries in
  each M/R/B/J/Q/U family, page4, three alternating reset/progress pairs. Keep
  current generation and unrelated keys. Pin the same old-history snapshot;
  disable compaction and hold cache/write-buffer options constant in both arms.
  Attribute C and members separately using actual native seeks/internal/deleted
  skips, block reads/bytes and seek/next time. Process CPU ticks cover the entire
  stage/segment. This deliberately retained-history diagnostic does not measure
  the admitted production adapter, SQL throughput or production resource pools.
- Actual operator cleanup: 12 groups ×3 obsolete generations ×16 entries in
  each family, configured aggregate page4 and unchanged native-test resource
  pools, actual RocksLiveState and AggregateStore. Retain current members and
  due pointers owned by generation3. Time bounded cleanup calls and report
  completed generations/pages in early/middle/late thirds; exact assertions
  independently verify all obsolete deletion and all current retention. This
  is a finite operator diagnostic, not a full SQL/capacity qualification.

The operator profile is identical across pins: maximum logical key/value
256/16,384 bytes; aggregate scan page 65,536 bytes/four entries; write batch
524,288 bytes/64 operations; overlay 524,288 bytes. Rocks cache and write buffer
are each 1 MiB; queued-write and decoded-value pools each 4 MiB; scan pool
1 MiB; two blocking operations, two snapshots and one open database. Disk
reserve is zero for these temporary fixtures. Normal compaction remains enabled.
An identical initial snapshot remains pinned through each arm. Only cleanup
calls are timed; fixture completion reads and exact final oracles are outside
the timer. This is debug test-profile measurement, not optimized SQL execution.

Evidence root: `/home/jason/qa-evidence/str116-cleanup-20261015/`.
Disk check before runs: 151 GiB available on the shared filesystem.

## Repair and bounds

One optional, admitted progress record belongs to one serial cleanup drain. It
holds at most two exact-length logical keys, a phase and completion flag; its
permit charges twice the configured maximum logical key size. It is released
at drain completion, cancellation/error, owner replacement and recovery. It is
never checkpointed. Each new drain or fresh owner starts from durable C markers.
Updates insert C markers with the established generation transition; committed
cleanup removes exhausted markers with the established atomic write. The serial
operator does not insert members into obsolete generations during its drain.

C discovery keeps the original bounded first-entry lookup when progress is
unavailable. With admitted progress it point-reads the active C marker, then
seeks exclusively after a completed or disappeared marker. Member discovery
retains the existing escaped namespace/family/group/generation prefix upper
bound and configured page cap, adding an exclusive lower bound after the last
committed member. Marker/phase checks invalidate a member bound at transitions.
T's same-group later-generation lookup and its two-step keyed pair deletion
then marker retirement remain unchanged. There are no new scans or indexes.
Every cursor update follows acknowledged commit; cancellation after an actual
commit can repeat safe discovery without publishing unacknowledged progress.

Optional admission first probes the existing mandatory decoded peak while
reserving the two-key charge. The peak includes the scope overlay, three scope
values and the existing Rocks read reservation (one value, two encoded keys
and result containers). Existing pressure therefore denies the optional cursor.
If pressure arrives later, decoded-value exhaustion releases the cursor after
the failed scope unwinds and retries cold once. Other resource/error classes
retain their original result. Tests cover both shared-pressure boundaries and
exact pool release. Pool/page/profile limits are unchanged.

U formerly divided the scan-entry cap by two alongside write capacity, so a
one-entry page refused even when two deletes plus a marker write fit. The repair
divides only operation/byte/overlay capacity for the possible pointer and owned
due deletion, then takes the unchanged scan-entry minimum. Owned H removal and
U removal remain in the same admitted atomic batch; replaced/current H survives.
The preserved baseline test demonstrates the old refusal with unchanged state.

## Completed baseline and native evidence

`baseline/receipt.json` binds the untouched source inventory, development image,
compiled artifact and pinned worker executable. Actual operator baseline passed
three repeats: each deleted 3,456 obsolete members and 36 markers, retained
1,152 current members and 192 owned current due entries, and completed twelve
generations per early/middle/late third (420 pages per third). Timed thirds in
seconds were 0.397/0.540/0.659, 0.431/0.592/0.631 and
0.460/0.619/0.736. Raw process peak RSS was 95,216–97,336 KiB.

`native-diagnostic-retry.log` records three matched alternating native pairs.
Each arm deletes exactly 10,368 members plus 72 markers and retains 3,480 keys.
C deleted-entry skips fall from 109,908 to 3,053; member deleted-entry skips
fall from 36,288 to 2,592 in each repeat. C reset early/middle/late skips are
11,868/36,636/61,404 versus progress 989/1,032/1,032; member reset skips are
12,096 per third versus progress 864. Native issued/internal/child seeks, block
reads/bytes, seek/next nanoseconds, elapsed time and process CPU ticks are
recorded separately for C and member traversal in the raw log. Issued seeks
remain 1,032 C and 1,008 member seeks per third in both native arms: the repair
changes lower bounds, not this diagnostic's seek count. For repeat zero, late
C internal skips are 259,908 versus 7,224, while member internal skips are
15,552 versus 4,320; the raw log preserves child/memtable and timing variability.

This mechanism diagnostic uses a native iterator per page, a retained old
snapshot and disabled compaction. Production performs admitted logical-entry
reads and retains the ordinary compaction profile; its C point-read behavior
also differs. These native counters demonstrate repeated traversal, not actual
operator timing or SQL throughput. Actual cleanup measurement is separate.
The first native fixture failed because a complete encoded-key terminator was
used as a prefix; the correction removes that terminator to match production
prefix encoding. Original failure/source/receipt remain in `native-diagnostic.log`,
`native-failed-source.rs` and `native-failed-receipt.json`.

## Exact test coverage

The worker tests use actual admitted stores, including fresh memory and RocksDB
owners, rather than a native-only simulation. The recovery matrix covers M, U
and keyed T persisted intermediate states on both backends at page sizes one
and four. It checks all six member families, current generation visibility,
calendar due ownership, durable totals, group/live-row pair retirement and
calendar values [2,1,2] followed by [1,0,1] after a real correction.

Cancellation tests seed an acknowledged partial M page before gating the next
actual commit before/after application. They preserve the prior cursor until
acknowledgment and resume using both the same cursor and a fresh one, including
M-to-R transition and unkeyed T marker deletion. Keyed pair retirement is
separately covered by persisted T recovery. Write refusal
also begins from nonempty progress, verifies exact unchanged state/cursor, then
retries. Snapshot-wait cancellation and optional-admission pressure assert
permit lifetimes. Other boundary cases cover lower C markers appearing in a
new drain, later generation protection, populated cold fallback, insufficient
U operation/byte/overlay capacities, exact one-entry atomic pointer/due removal,
and the maximum-key Rocks decoded read peak. All these tests passed in the current worker target described below.

## Current acceptance

The comprehensive `candidate-headroom/receipt.json` inventory passed locked
worker/state production checks, strict state Clippy, formatting, 14 Rocks tests
(three ignored), and the explicit native diagnostic. Worker strict Clippy and
worker build/test targets shared one fixture-only moved-value compilation
failure. It was corrected by making mutually exclusive classification branches
an else-if; all failures and exit codes remain preserved.
The next `candidate-fixture-repair/receipt.json` inventory passed the worker
build, formatting and three aggregate-store tests; incremental tests passed 57,
failed three and ignored two. Its single strict Clippy finding was a pilot
modulo expression, corrected to `is_multiple_of`. Runtime failures occurred
before the target cancellation operations: two fixtures retained a template's
sole database permit while opening its replacement; a snapshot fixture tried
two full write scopes that exceeded the unchanged queued-write pool. Templates
now release before replacement; snapshot saturation uses two real snapshots
without duplicate write scopes, preserving the pending third-snapshot wait and
queued-write release assertions. All original logs/exits remain preserved.

The final `candidate-final/receipt.json` inventory passed strict all-target worker
Clippy, formatting, freshly compiled worker test pin, three aggregate-store
tests and 60 incremental-aggregate tests (two ignored). Explicitly running the
finite pilot produced all six successful matched arms. The preserved state
source is identical to the successful state receipt. Production worker code is
also identical to its passed locked check; subsequent edits were test fixture
repairs and the pilot lint. No unrelated failure was repaired.

| Validation command (through queued development wrapper) | Exit / result |
| --- | --- |
| `cargo-dev check --locked -p arroyo-worker` | 0 |
| `cargo-dev check --locked -p arroyo-state` | 0 |
| `preflight-clippy.sh -p arroyo-worker` | 0, strict all targets |
| `preflight-clippy.sh -p arroyo-state` | 0, strict all targets |
| `cargo-dev fmt --all -- --check` | 0 |
| Worker lib `test --locked --no-fail-fast --no-run --message-format=json` | 0, fresh artifact pinned |
| Worker lib `arrow::aggregate_store` serial | 0, 3 passed |
| Worker lib `arrow::incremental_aggregator` serial | 0, 60 passed, 2 ignored |
| State lib `live::rocks::tests` serial | 0, 14 passed, 3 ignored |
| State lib ignored `cleanup_generation_churn_diagnostic` | 0, three native pairs |
| Pinned ignored worker `native_cleanup_generation_pilot` | 0, three alternating pairs |

All Cargo test commands use `--locked --no-fail-fast`, and serial targets use
`--test-threads=1`. Logs retain exact full arguments, exits and duration. Every
build, pin and dependent run was foreground within one shared reservation;
executables/source inventories and receipts remain outside regenerable Cargo
cache. Both baseline and candidate compiler artifacts report `fresh=false`;
the common development image is recorded in their receipts. Baseline pin SHA256
is `f73f7b4818a97926e0c9e1032574df9612bb56749deb95388152b818d81b4175`; candidate pin SHA256 is
`ef845696afc0d94bea32a342ebd87db40e211e4ae0ec72970ec7610aebc58c6d`.

## Matched actual operator result

Each arm completed 36 generations, deleting exactly 3,456 old members and 36
markers while retaining the same 1,152 current members and 192 current due
entries. Each timed third completes twelve generations/1,152 members/twelve
markers. Pins alternate baseline/candidate, candidate/baseline,
baseline/candidate. Values are bounded cleanup-call seconds, not fixture setup
or external process recovery.

| Repeat | Baseline early/middle/late seconds | Candidate early/middle/late seconds |
| --- | --- | --- |
| 0 | 0.423395 / 0.571488 / 0.809043 | 0.377358 / 0.501626 / 0.620652 |
| 1 | 0.400729 / 0.553810 / 0.687578 | 0.395414 / 0.481164 / 0.507698 |
| 2 | 0.405697 / 0.499268 / 0.600265 | 0.366864 / 0.417954 / 0.433005 |

Baseline pages are 420 per third; candidate pages are 372. This reduction comes
from U's repaired admitted chunk arithmetic (four scan entries can now process
four pointers when write capacity fits, previously two). The drain cursor
changes traversal work within those cleanup calls. Equal logical completed work is the
comparison denominator. Actual timings measure the combined repair and do not
attribute a separate speedup to C versus members. All finite candidate totals
are lower in these three pairs (about 16–19%), with noisy timing and no claim
of stable production throughput. Paired per-arm raw peak RSS is 95,168–95,828 KiB for
baseline and 95,808–96,140 KiB for candidate; no process-memory reduction is
claimed. The resource-bound assertions, rather than RSS alone, establish the
optional two-key charge/lifetime.

## Review and remaining scope

Source reviews approved the production headroom repair and subsequent fixture
repairs (`admission-repair-source-review.md` and coordinator's fresh fixture
review). Final source/pin/evidence review, publication and CI remain with the
coordinator. Original failures, corrections, every selected inventory and
matched logs remain in the evidence root. No pending user decision or local
implementation/test gap remains.

Full SQL, external worker/controller-process recovery and 1.2M/combined
milestone qualification are deferred to their separate acceptance batch. The
exact recovery tests here instantiate fresh backend/operator owners using the
existing worker harness and persisted M/U/T phase state; they do not claim that
broader process qualification.
