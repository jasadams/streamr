# RocksDB read views: approved lifecycle

The user approved this lifecycle change on 2026-10-10 after discussion of reader
retention and shutdown tradeoffs. STR-72 implements it; validation is recorded
in the milestone ledger. This concerns native aggregate execution on disk-backed
state. The
optional per-group emission policy in STR-44 remains post-MVP.

## Observed problem

The v10 RocksDB capacity query groups 100,000 keys with 8,192-byte retained
values using ordinary SQL:

```sql
SELECT k, COUNT(*) AS n, MAX(payload) AS max_payload
FROM aggregate_input
GROUP BY k;
```

The default queue configuration exhausted the shared execution budget. A
separate diagnostic uses the existing queue-size setting of 32 rows with the
same input, resource limits and oracle. Its initial execution reported 100,000
output rows after approximately 27 minutes. The capture then exceeded its
declared 1,800-second deadline during the checkpoint/recovery execution. Exact
initial/recovered values and the ten-times checkpoint gate remain unqualified;
the matching leader case did not run. Owned-process cleanup completed, and the
source/build/binary/helper pins remained unchanged. SQL process RSS samples
remained approximately 200–211 MiB.

Source inspection identifies repeated physical snapshot setup during result
draining. Each bounded output scope calls `AggregateStore::begin`. This case
admits at most 20 dirty groups per scope, or fewer when previous and new values
fill the output allowance. A 100,000-group drain therefore requires at least
5,000 views. `RocksLiveState::snapshot` creates a local physical RocksDB
checkpoint and opens a separate read-only database for each view. The pinned
binding passes a zero flush threshold. This is not an Arroyo durable recovery
checkpoint, but it incurs flush/checkpoint/open/cleanup work. Source inspection
establishes that repeated work; it does not attribute all measured runtime or
I/O to it.

Current evidence is retained in
`target/native-m3-aggregate-many-queue32-diagnostic-v10/`. The report
`/tmp/streamr-rocks-native-snapshot-feasibility.md` records pinned dependency
sources, callsites and ownership constraints.

## Why the previous adapter used physical views

The previous adapter introduced a stronger read-view guarantee: a snapshot remained readable
after its original live database has been explicitly closed and removed. Earlier documentation and three RocksDB tests promised that behavior; the
approved lifecycle replaces it for ordinary views. Independent local
checkpoint directories provide it.

No production caller requiring reads after a completed explicit close/removal
was found in the repository audit. Native operators release their scoped views;
checkpoint exports retain views until completion or cancellation. This audit
does not establish what external Rust consumers might depend on. The lifecycle change was explicitly agreed with the user.

Sharing one view across an entire result drain is not a safe substitute. Holding
its permit during downstream delivery can deadlock two operators sharing a
single allowed snapshot. Current scopes release ownership before delivery.

## Approved contract

Use native RocksDB read snapshots for the existing stable-view API. A view keeps
its live database alive until its last reader and in-flight operation finish.
Callers release views before awaiting explicit close/removal.

| Behavior | Previous physical views | Ordinary native views |
| --- | --- | --- |
| Read the old value after a live update/delete | Supported | Preserved |
| Stable paging and cloned readers | Supported | Preserved |
| Await close/removal while retaining a view | Completes independently | Waits for the view to be released |
| Read through a retained view after completed removal | Supported | Withdrawn |
| Release the live database slot and delete its path | Can precede view release | Follows final view/native-operation release |

The benefit is avoiding physical checkpoint creation for ordinary short-lived
query reads. Long readers would delay database cleanup and fresh-attempt
admission at the open-database limit. Native snapshots retain historical
versions during compaction; snapshot count does not bound reader age or retained
disk history. These costs need measurement under the existing limits.

The pinned Rust RocksDB binding exposes borrowed snapshots. STR-72 uses
`self_cell` 1.3.0 to own an `Arc<NativeDb>` and its borrowed snapshot safely.
Reads and paged iterators use the captured sequence. Native release runs on the
bounded cleanup executor before database-owner release; cancelled native work
retains its view and admission until it completes.

This changes Streamr's introduced backend lifecycle contract. Existing SQL,
Arroyo Parquet checkpoint writers, formats, coordination and recovery remain the
integration path. Minimum filesystem headroom, operation/read/page/cache and
snapshot limits must remain enforced; admission accounting must reflect the
actual native-view costs.

Durable capture uses the generic `checkpoint_snapshot` method. RocksDB keeps
its physical flush/checkpoint/read-only database path there; memory delegates to
its existing stable view. The table-manager barrier captures this independent
view before starting asynchronous Parquet export. Formats and publication remain
unchanged. Existing physical-capture tests retain independent readability after
live removal; ordinary-view tests instead require close to wait for final readers.

## Verification after agreement

Keep exact stable values across updates/deletes, paged/cursor isolation, cloned
readers, read bounds and cancellation. Test explicit close/removal waiting on
real readers, final database lock/slot/path release, and open-database and
snapshot limits of one. Preserve bounded cleanup and error propagation.

Run the required combined Bookworm gates, existing checkpoint/export/recovery
tests and unchanged native SQL oracles. The separate STR-32 qualification batch must repeat the full capacity workload
with the same budgets and record snapshot time, RSS, disk/compaction and actual
checkpoint/restored-value evidence. Faster creation alone does not qualify
milestone 3.
