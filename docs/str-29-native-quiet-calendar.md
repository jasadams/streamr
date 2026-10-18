# Native quiet calendar delivery

STR-29 repairs missing production dispatch, preserving its approved event-time
contract. On native SQL calendar COUNT FILTERs, keyA's lifetime/today result
changes from 1/1 to 1/0 when keyB advances the finite watermark into tomorrow.
Silence, Idle and terminal EOF do not invent calendar progress. No application
UDF, synthetic event, custom timer graph, admission pool or public API is added.

PR #23 merged main at 2026-10-10 05:14:42 UTC. PR #30 then merged into #23's
branch at 05:16:18. Its scheduler did not reach main. Current fetched base
`14353046e5085325b66563981be61c96ecfed7b7` had H/U deadlines and recalculation,
but only test callers of that callback. Historical passing tests did not prove
production delivery.

The existing aggregate watermark handler now records monotone W/V pruning,
flushes accepted updates/removals, dispatches due owners and drains ordinary CDC
before forwarding progress. Each dispatch atomically verifies generation and
H/U ownership, recalculates G, reschedules H/U and marks D. The scope closes
before collector waits. Output retains the existing event timestamp contract.
A restored or regressing notification uses persisted real W's UTC day. Pending
H or committed D survives existing checkpoint/replay machinery.

## RocksDB enumeration

Unknown quiet keys require ordered enumeration; they cannot be discovered with
known-key point reads. One H scan runs per due/stale owner plus a final empty
probe on each eligible finite notification. G, U and W use point reads.

Logical H lower bound is prefix `H` on a cold notification, then the last
committed deadline key in that serial drain. The existing scan seeks inclusively
and rejects keys at or below that continuation. Exclusive upper bound is
`H || sign_order_be(W + 1)`, so deadlines equal to W qualify and future owners
are excluded inside RocksDB. At signed maximum the H prefix successor supplies
the upper bound. Physical bounds use the existing table namespace plus escaped
logical bytes; the native upper bound is the minimum of the explicit end and
prefix successor. Partition/subtask ownership stays in that namespace.

Each call uses existing page entry/byte limits and one scope snapshot. Only one
owner is mutated, with five bounded writes; extra returned entries remain a
bounded page. The snapshot/page reservation drops before normal output awaits.
Deletion/rescheduling and G/D updates commit together. A transient continuation uses existing optional backend decoded-key admission;
if headroom is unavailable, the original cold scan remains correct. It advances
only after commit. No input interleaves with this serial drain and replacement
deadlines lie beyond W. Cancellation/recovery/new notifications start cold; no
continuation or snapshot enters a checkpoint. Bounded returned rows are not a
bound on physical backend traversal. This change makes no production performance
improvement claim.

The activated existing B recalculation scans remain bounded by the known group,
generation and aggregate family. Their logical continuation starts after
first-retained-day-minus-one (the native seek is inclusive and the merge rejects
keys at or below the continuation), with exclusive reference-day-plus-one upper
bound. Lifetime and correction metadata remain.

## Validation

The focused SQL fixture uses supported `EVENT_DATE()` in native COUNT FILTER.
Source controls first hold at keyA1/1 + keyB1/1, then at keyA1/0 + keyB2/1.
Both states are observed before EOF and remain byte-identical through silence.
Initial and freshly restored workers must produce exact CDC before images and
exactly one keyA 1/1 -> 1/0 transition. Memory/RocksDB and controller/leader are
separate cases. This is local SQL-worker recovery, not Kafka/process-loss or
full capacity qualification.

Before integration, all four SQL cases passed, as did formatting, strict lint,
locked check, 28 calendar tests, 6 idle-control and 3 capture tests, plus 12 Python
checks. One pre-existing calendar diagnostic remained explicitly ignored. Final integrated source additionally passes formatting, strict Clippy and
locked affected-crate check; 29 calendar tests (1 existing diagnostic ignored),
6 idle-control and 3 capture helper tests, and 12 Python checks. All four actual
native SQL cases pass again on main `8f937aa9` plus the continuation repair.
The two helper filters overlap; their counts must not be summed as unique tests.

A supporting existing raw RocksDB diagnostic passed 18 finite arms (54 progress
records): 600 selected + 600 unrelated keys, three repeats, sizes 16/96-byte
keys and 32/1024-byte values, pages 1/16, retained pre-delete snapshot and disabled
automatic compaction. The restart-prefix arm showed 100,100 internal skips and
99,900 deleted skips for its last 200 page-1 owners in repeat 0. This concrete
mechanism evidence motivated continuation within a serial drain. It is not an
actual H-dispatch benchmark; exact settings, seeks, block reads/bytes and timings
are retained in `raw-scan-summary.json`.

Current commands, exits, source patches and captures are preserved in
`/home/jason/qa-evidence/str29-native-delivery-20261018/`. Final integrated executable validation passed; current-head PR CI remains a
publication gate. Finite owner/churn tests cover both backends and one-entry pages.
The initial four-case SQL run took 55.44 seconds; its child-process diagnostic
reported 1.38 user / 0.35 system CPU seconds, 174,788 KiB maximum child RSS and
208/2576 input/output blocks. These are local harness diagnostics, not production
peaks or performance qualification. Actual H internal/deleted-key traversal is
unmeasured and must not be inferred from returned rows or the raw diagnostic.

## Shared-target integration retry

On integrated main `8f937aa9`, strict Clippy passed but locked check reported
`OperatorContext.restore_commit_epoch` absent while the current source defines
it at `crates/arroyo-operator/src/context.rs:519`. The check reused metadata
from a different source revision in the shared `/app/target` paths. The failed
check log remains in `integrated-final/check.log` (exit 101); executable tests
were blocked in that attempt. The queued retry refreshes only timestamps of
Rust/protobuf crate files changed by that main integration or this feature, verifies identical
content hashes, and preserves native archives and shared Cargo/compiler caches.
Exact paths/hashes are in `integrated-cache-refresh/mtime-refresh.json`.

The integrated timestamp-refresh retry passed every selected gate (exit 0).
Its current test executable SHA256 is
`e414a7fcb22eac8cc14d1539a6b51d82ff06ae1db731f97298cb5bc5da81788f`;
`integrated-cache-refresh/validated-files.json` binds code to gates and
`runtime-integrated/` preserves four successful captures and resource receipts.
Original and integrated pinned executables remain outside Cargo caches.

[PR #75](https://github.com/jasadams/streamr/pull/75) targets main directly.
Independent source/evidence review approved with zero findings. Current-head CI
is pending; merge-sweeper owns the merge and Done transition.
