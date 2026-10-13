# Historical native aggregate checkpoint inventory evidence

These results describe the checkpoint and aggregate representations at their
recorded historical source revisions. They do not qualify the current checkpoint
format or current aggregate value sizes. Use
[native retained-checkpoint measurement](native-retained-checkpoint-measurement.md)
for the current format-2 Parquet object inventory and caller-projected row checks.
That object inventory does not decode native aggregate groups or establish their
logical G/M/R counts.

The historical inventories validated selected controller or leader metadata,
namespace/owner/epoch/generation, table schema identity, safe object paths,
SHA-256 and byte/row counts, sorted unique keys, and recognized key layouts.
They reported exact `G/D/M/R/E/C` counts and stored value-length min/max/sum per
operator; they did not decode Arrow IPC or prove output values.

The observations below retain their historical sequence, including checks that
were pending at each point. "Fresh combined source" refers to the archived
validation source, not the current checkout. Obsolete tooling and its invocation
instructions have been removed; captured inventories and the measured evidence
remain source-specific.

For the one-key current-result UNION query with `SET updating_ttl = NULL`, a
settled snapshot should have no `D/E/C` entries. An actual memory/controller
epoch-1 checkpoint after the first source event showed lifetime `G=1`, latest
closed-HOP `G=0`, and outer retractable `MAX` group `G=1,M=2,R=2`. The outer
aggregate indexes a member for **each aggregate expression**, including the
NULL value supplied by the other UNION branch. Once both lifetime and latest
closed-HOP branches have one live row, the expected steady outer shape is
`G=1,M=4,R=4`, provided prior branch values retract cleanly. This is a proposed
bound, not yet full-state evidence: inspect a checkpoint after many closed
windows and updates, and check that the indexed member count stays at four.
The latest closed-HOP aggregate should then have `G=1` and no `M/R` if its
compiled mode remains append-only. Correct final output or constant RSS alone
does not establish this bound.

A separate four-event retention probe passed the memory/batch-1
controller and leader checkpoint inspections: each of three owners had `G=1`,
and the outer aggregate had `M=4,R=4`. Its batch-8 companion expected recent
count 2 but observed NULL because watermark generation takes the minimum event
time within each source batch, including values `[1,3,5]`. This is an invalid
fixture expectation for that batch shape, not evidence of an engine defect.

The earlier 16-scope retention attempt comprised eight ten-event retraction
captures (memory/RocksDB × batch target 1/8 × controller/leader) and eight
many-window captures (memory/RocksDB × 64/4096 groups × controller/leader at
batch target 8). Fourteen scopes passed at that source snapshot: all eight
retraction cases, all four many-window memory cases, and both 64-group RocksDB
cases. The 4096-group
RocksDB controller and leader were then outstanding.

For both memory checkpoint protocols, inventories at 64 and 4096 groups are
identical: each of three aggregate owners has `G=1`; latest-result, lifetime
and outer `G` values are 1,840, 1,456 and 1,200 bytes respectively. The outer
owner has `M=4` values of 9 bytes each and `R=4` values of 44 bytes each; total
logical value bytes are 4,708. This demonstrates the fixture's constant
checkpoint shape across those cardinalities. The revised tail fixture's 4096-
group memory captures pass with latest count 2 for all last-batch partition
lengths 1–8. An earlier prefix with count 1 had no established cause.

The first 4096-group RocksDB/controller attempt timed out at 120.24 seconds
before initial capture completion; its incomplete log is preserved at
`target/native-result-retention-tail-qualified/many-4096-rocksdb-8-controller/capture.log`.
Source task completion at 1.6 seconds and lifetime aggregate completion at
55 seconds do not establish a completed capture. A second controller attempt
used the explicit `STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=900` override and timed
out after 900.27 seconds before any checkpoint. In that run, source ended at
1.6 seconds and lifetime aggregate at about 30 seconds; window/latest/outer
aggregate tasks did not finish. The incomplete log is preserved at
`target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller/capture.log`.
The serial runner stopped at controller failure, so leader did not run. These
attempts are not completed captures or performance passes.
That earlier source qualified fourteen of sixteen scopes; the two 4096-group
RocksDB modes had not completed at that point.

The fresh combined source qualified all 16 scopes at
`target/native-result-retention-snapshot-fixed`. Its eight retraction cases
span memory/RocksDB, source batch targets 1/8, and controller/leader
checkpoints. Its eight many-window cases span memory/RocksDB, 64/4096 groups,
and controller/leader checkpoints at batch target 8. Each case has a completed
initial capture, committed epoch-1 checkpoint and fresh-worker recovered
capture; the fixture asserts exact checkpoint/final values and CDC before
images. The read-only
`target/native-result-retention-snapshot-fixed/comparison-manifest.json`
records each case's capture/inventory SHA-256, selected job/epoch, per-owner
prefix counts and value lengths. The inventory's selected metadata job ID
matches the recovered capture's job ID.

In each of the four backend/protocol comparisons, the 64- and 4096-group
selected aggregate inventories are identical:

| Owner | Logical prefix counts | Stored value lengths | Total value bytes |
| --- | --- | --- | ---: |
| lifetime `updating_aggregate_4` | G=1; M/R/D/E/C=0 | G=1,456 | 1,456 |
| latest closed-window `updating_aggregate_12` | G=1; M/R/D/E/C=0 | G=1,840 | 1,840 |
| outer result `updating_aggregate_17` | G=1, M=4, R=4; D/E/C=0 | G=1,200; M=4×9; R=4×44 | 1,412 |
| **Selected aggregate total** | **11 keys** | | **4,708** |

The 64-group checkpoint follows 101 real input rows; the 4096-group checkpoint
follows 6,149. This establishes constant retained **selected aggregate** key
counts and stored value bytes for this fixture, not a general memory bound for
all queries. The inventory excludes the fixed-window, source and sink state,
encoded keys, backend overhead, remote physical bytes and RSS. It also does
not prove thousands of separately flushed CDC replacements or idle-key zero
emission. Both previously missing 4096-group RocksDB modes completed initial
and recovered captures in 166.55 seconds (controller) and 143.94 seconds
(leader) total. The prior 120.24- and 900.27-second controller timeouts remain
historical incomplete runs, not controlled performance baselines.
