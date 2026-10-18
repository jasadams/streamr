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

Logical H lower bound is prefix `H`; exclusive upper bound is
`H || sign_order_be(W + 1)`, so deadlines equal to W qualify and future owners
are excluded inside RocksDB. At signed maximum the H prefix successor supplies
the upper bound. Physical bounds use the existing table namespace plus escaped
logical bytes; the native upper bound is the minimum of the explicit end and
prefix successor. Partition/subtask ownership stays in that namespace.

Each call uses existing page entry/byte limits and one scope snapshot. Only one
owner is mutated, with five bounded writes; extra returned entries remain a
bounded page. The snapshot/page reservation drops before normal output awaits.
Deletion/rescheduling and G/D updates commit together. Restarting from H after
commit resumes safely through cancellation/recovery without a volatile cursor.
It can revisit deleted internal keys; bounded returned rows are not a bound on
physical backend traversal. This change makes no performance improvement claim.

The activated existing B recalculation scans remain bounded by the known group,
generation and aggregate family, with retained horizon lower bound and exclusive
reference-day-plus-one upper bound. Lifetime and correction metadata remain.

## Validation

The focused SQL fixture uses supported `EVENT_DATE()` in native COUNT FILTER.
Source controls first hold at keyA1/1 + keyB1/1, then at keyA1/0 + keyB2/1.
Both states are observed before EOF and remain byte-identical through silence.
Initial and freshly restored workers must produce exact CDC before images and
exactly one keyA 1/1 -> 1/0 transition. Memory/RocksDB and controller/leader are
separate cases. This is local SQL-worker recovery, not Kafka/process-loss or
full capacity qualification.

Current commands, exits, source patches and captures are preserved in
`/home/jason/qa-evidence/str29-native-delivery-20261018/`. Executable validation
is pending. Finite owner/churn tests cover both backends and one-entry pages;
physical internal/deleted-key traversal, CPU, I/O and RSS are not yet measured.
They must not be inferred from returned-row counts or historical diagnostics.
