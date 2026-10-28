# Native incremental RocksDB checkpoint direction

On 2026-10-28 the user requested the full performant solution after the STR-162
review, setting aside the unchanged-Parquet-copy candidate. The proposed target
is native incremental RocksDB checkpoints with complete recovery and shared-file
retention, the core outcomes of STR-25 and STR-24. The user agreed to this
architecture on 2026-10-28; the concrete
[implementation plan](str-25-native-checkpoint-plan.md) now governs execution.
Implementation and scoped correctness/recovery gates pass, including fresh-process
recovery from the measured large state on normal dev/test and optimized pins. The completed
[coverage verification](str-25-native-checkpoint-verification.md) passes every
phase-count and native-compaction gate with independent KEEP. Mean sampled CPU
is 60.8% lower and idle checkpoint median is 3.978→0.110s within matched arms.
The original first experiment remains INCONCLUSIVE; its evidence is preserved.
See [measurement](str-25-native-checkpoint-measurement.md) and
[validation](str-25-native-checkpoint-validation.md) for source bounds and limits.

Source is `8cddb321b631b79185bf6d166a0fdf9522db529f`; guidance and worktree are
recorded in [STR-162 review](str-162-export-reuse-review.md). Current full tickets
STR-25, STR-24, STR-23, STR-12 and STR-13 were read. Their old design-file reference
`docs/disk-backed-state-design.md` is absent at this base. Current repository
pre-release and MVP instructions govern; historical dependency status does not
establish current code completeness or authorize rescaling/migration work.

## Flink reference

Flink uses a native RocksDB checkpoint directory during synchronous preparation,
then uploads files asynchronously. Its incremental strategy reuses eligible SST
handles from completed checkpoints, and its shared-state registry tracks their
lifetime. Native recovery avoids rebuilding the database by replaying exported
key/value rows. Compaction can produce new SSTs containing old logical state,
so incremental upload size is physical file churn, not simply changed-row size.
Flink also offers a separate changelog approach to reduce compaction-driven
checkpoint tails, at extra continuous CPU, memory and I/O cost.

Primary references, inspected 2026-10-28:

- [Flink state backend documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/), currently reporting 2.3.0.
- [Flink release 2.3 native snapshot preparation](https://github.com/apache/flink/blob/release-2.3/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/snapshot/RocksDBSnapshotStrategyBase.java), `syncPrepareResources` and `takeDBNativeCheckpoint`.
- [Flink release 2.3 incremental strategy](https://github.com/apache/flink/blob/release-2.3/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/snapshot/RocksIncrementalSnapshotStrategy.java), completed-checkpoint reuse and upload selection.
- [Flink release 2.3 shared-state registry](https://github.com/apache/flink/blob/release-2.3/flink-runtime/src/main/java/org/apache/flink/runtime/state/SharedStateRegistryImpl.java), registration and `unregisterUnusedState`.
- [Pinned RocksDB 10.4.2 checkpoint implementation](https://github.com/facebook/rocksdb/blob/v10.4.2/utilities/checkpoint/checkpoint_impl.cc), complete file capture, hardlink/copy and directory completion.

These release branches are inspected references, not frozen benchmark sources.
The previously accepted Flink throughput comparison used nonincremental RocksDB
checkpoints; its advantage cannot be attributed to incremental snapshots.

## Proposed complete product contract

1. Capture one native database per operator owner at the barrier, covering its
   entire registered namespace/schema roster. All tables already share a backend
   (`tables/table_manager.rs:817-825,918-1039`). Capture must complete before
   post-barrier processing resumes. An existing MVCC reader is not a native
   checkpoint directory and cannot be converted later without changing the cut.
2. Use the pinned RocksDB checkpoint API under write exclusion and existing
   resource admission. Worker writes disable WAL (`live/rocks.rs:35-49,91-104`),
   so memtable data must be flushed into the selected native checkpoint.
   Retain the API's complete required file set and metadata; never upload only
   arbitrary live SSTs. Stage on the same filesystem so immutable SSTs can be
   pinned with hardlinks. Upload proceeds asynchronously from the staged view.
3. Upload new immutable SSTs and reuse verified eligible objects from a globally
   completed checkpoint. Do not use raw RocksDB file numbers as global identity
   or treat worker upload completion as global completion. Each manifest describes
   the complete native file set and owner/config/format identities needed for
   independent restore; it is not an instruction to replay a chain of row deltas.
   Store small mutable native metadata privately for each checkpoint.
4. Add shared-file ownership and reachability to both controller and leader
   cleanup before enabling reuse. Protect all retained/recovery roots and active
   publication, handle generation takeover, and reclaim failed-upload orphans.
   Current exclusive-epoch deletion is unsafe for shared SSTs. Preserve existing
   canonical publication/fencing and Kafka commit/replay machinery. A concrete
   race-safe bounded GC algorithm and completed-base notification must receive
   independent review before implementation; no assumption that an in-memory
   reference counter survives controller loss.
5. Download and verify selected files into a fresh attempt directory before
   opening the managed database. Construct fresh local ownership metadata, use
   configured cache/WBM/native options and Checkpoint durability, and bind all
   logical tables to that one restored backend. Current factory opens empty DB
   before per-table row import (`live/worker.rs:55-96` and table registration);
   native recovery needs an explicit construction/restore seam. Diagnostic local
   reopen uses different durability/ownership and is not that seam.
6. Bound buffers, transfer concurrency, manifests, staged-file lifetimes and disk
   reserve under existing backend resources. Account for hardlinked files pinned
   during compaction, cancellation and slow uploads. No whole-state RAM cache,
   per-row export/recompression in the native path, or resource-limit increase.
   A reused SST must not be fully rehashed every checkpoint merely to rediscover
   its unchanged identity; prove that immutable identity/verification is sound.

Memory state keeps its existing logical checkpoint path. The proposed initial
native path preserves current same-ownership/singleton semantics and explicitly
rejects unsupported ownership/version changes. Building distributed rescaling,
old-checkpoint migration or a changelog backend is a separate scope decision.
Portable current-state export can remain where it serves an actual supported
capability; pre-release policy does not require obsolete-format readers.

The storage investigation proposes a bounded mark/sweep contract: mark the union
of all retained manifests, selected recovery bases and canonical published roots
awaiting commit completion. Sweep only unmarked objects born before a fixed
prune cutoff whose birth owners are fenced against publication. Object identity
includes upload birth ownership and verified contents. A future checkpoint may
reuse its confirmed head/base objects or create new identities beyond that
cutoff; it cannot resurrect arbitrary old unretained objects. This must apply
to normal and abandoned-upload cleanup. An uncertain root/frontier stops deletion.
Persist complete manifests, preserve ready/canonical fences, and keep deletion
retry discoverable before pruning the final candidate-recording manifest. Bound
the retained-root union and spool candidates if needed; no unbounded object map.

The capture/restore and shared-GC reviewers approve this architecture direction
for user discussion. They do not claim a final implementation plan or passing
runtime tests. Required open design details include the completion notification
seam, exact root frontier across generation takeover, bounded metadata layout
and all concrete resource/measurement thresholds.

## Completion evidence

The sprint is not complete with SST upload alone. It requires native operator
continuation after fresh-process loss, exact multi-epoch unchanged/changed/
deleted/empty state, post-barrier writes, compaction, corrupt/missing files,
partial uploads, cancellation, stale generations, failed publication, Kafka
pending-commit replay and both cleanup modes. Shared objects must survive removal
of obsolete manifests and eventually disappear after their final live reference.
Retained state must exceed configured state memory and restore without row replay
or whole-state buffering. Checkpoint capture flush, upload and restore must obey
the existing managed backend limits.

Performance acceptance must exercise unchanged retained state, continuous sparse
updates, and deletion/compaction churn. Record completed work, checkpoint sync and
async latency distributions, new/reused/file-verification bytes, CPU, I/O, RSS,
local disk and native recovery time under matched limits and checkpoint cadence.
Measure the first full upload separately from steady state. Preserve idle costs
and compaction tails; do not present a microbenchmark or codec roundtrip as engine
qualification. Exact finite fixtures, sources and pass thresholds must be frozen
with three independent plan perspectives before code/builds under the
performance-spike workflow. No production deployment is implied.

## Decision boundary

The decision is to replace the RocksDB Parquet checkpoint transport with this
native incremental path and implement its shared-file retention and native restore
together, preserving native SQL and the existing global checkpoint/Kafka protocol.
This changes checkpoint representation, restore construction and GC ownership;
the repository requires agreement on that concrete architecture before product
code. Independent investigations are underway; a final implementation plan must
resolve publication/GC races and source pinning before claiming readiness.
