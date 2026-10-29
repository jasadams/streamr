# Native incremental checkpoint implementation plan

## Authorization and base

The user approved native RocksDB barrier capture, incremental immutable SST
uploads, shared-file retention and fresh-worker native restore together on
2026-10-28. This replaces the STR-162 Parquet-copy candidate. STR-25 owns the
implementation, including its STR-24 storage-lifetime prerequisite. Rescaling,
old-format migration, a changelog backend and production cutover are outside this
sprint. Existing supported cross-backend recovery remains a correctness contract.

Fetched base `8cddb321b631b79185bf6d166a0fdf9522db529f`; branch
`jason/str-25-native-checkpoints-20261028`; worktree
`/home/jason/repos/streamr-worktrees/str-25-native-checkpoints-20261028`.
Read `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, feature plan, current STR-25/24/162 and project.
In-flight STR-25 check CLEAR. Local main is 71 commits behind fetched main and
its changes are preserved. No signing or automatic ticket time limit applies.

Reuse source-bound diagnosis and reference analysis in
[native direction](str-25-native-checkpoint-direction.md) and
[STR-162 review](str-162-export-reuse-review.md). Full logical export currently
scans/recompresses retained state every checkpoint. The hypothesis is that native
capture and incremental file transfer remove that retained-row work during both
idle and changing state, with compaction-related transfer amplification retained
as a measured cost. No checkpoint interval or backend budget increase.

## Representation and interfaces

Add `optional NativeDatabaseCheckpointMetadata native_database` to both
`SubtaskCheckpointMetadata` and `OperatorCheckpointMetadata`. One operator owns
one singleton database covering its entire live-table roster. Existing global
and expiring tables retain ordinary metadata, including source offsets, pending
outputs and sink commit data. Native live tables keep configurations but emit
no fake empty logical table metadata. Memory live state keeps logical snapshots.
Native and logical live-state representations are mutually exclusive.

The native descriptor contains version 1, selected epoch/generation, singleton
subtask identity, database lineage/incarnation identity, native compatibility
identity, exact table/config/encoded-namespace roster, optional completed reuse
base identity, and a complete file list. Each file has a safe local basename,
remote immutable path, size, SHA-256 and shared classification. Only immutable
SST basenames can be shared; every required RocksDB checkpoint file is included.
CURRENT/MANIFEST/OPTIONS and required logs are private per checkpoint. Source
OWNERSHIP/LOCK/temporary paths are not remote recovery state. Validate safe names,
duplicates, exact roster, source job/operator/epoch/generation, native version,
encoding/comparator/features, required files and complete encoded RPC budgets.
The complete selected manifest is enough to restore; do not replay a row-delta
chain. Keep existing logical formats for the memory backend/current portable
recovery, without introducing obsolete-version compatibility shims.

Use native files under the originating checkpoint/operator prefix, in a distinct
`native-*` upload directory with a unique upload identifier. An SST's reusable
remote identity is its verified immutable object path, not the bare Rocks file
number or a global guessed content cache. New objects must be owned by the
current attempt/epoch/generation. A reused descriptor must match a file in the
authoritative completed base, including local name, path, length and checksum.
Validation at merge/publication must enforce this; trusting worker cache alone
is insufficient. Never overwrite an existing uploaded object.

Whole native descriptors remain bounded by the existing 3 MiB live-state RPC
ceiling and 65,536 file ceiling, with at most 32 live-table configurations.
Reject excess before publication and clean attempt-owned files. No SST-size
buffer or file-size cap inherited from small Parquet pages: stream larger SSTs
with finite buffers and declared-length/checksum verification.

## Barrier and completion

Add `LiveStateBackend::capture_native() -> Result<Option<NativeCheckpointCapture>>`
with default `None`. The capture owns a staged directory, finite file inventory
and existing capture/cleanup resources. TableManager captures once per backend
and stores either a native capture or the existing logical StateSnapshot.

RocksDB uses the pinned checkpoint API inside the existing bounded blocking
executor under `NativeDb.write_lock`. Worker writes disable WAL, so force
pre-barrier memtable persistence before the native checkpoint is established.
Use a fresh staging directory on the same filesystem for hardlinks; preserve
the API's complete checkpoint files, directory completion and failure handling.
Do not manually inventory live SSTs or derive a checkpoint later from an MVCC
reader. Resume processing only after capture; hashing/uploads are asynchronous.
The staging owner must clean both final and API-temporary paths even if its
caller is cancelled while the blocking closure is still finishing.

Piggyback `optional completed_base_epoch` on existing `CheckpointReq` (field 6),
the forwarded `CheckpointBarrier` and `CheckpointMessage`. RunningJobModel tracks
this epoch only after authoritative Ready or completed commit, including replay
on recovery. Advance at existing completion branches, not when workers finish
or `checkpoint_parent_ref` changes. Next-barrier notification promotes a matching
locally complete descriptor for reuse. Seed a restored owner only from verified
selected state. Failed uploads/publications never enter the reusable set.

Keep one bounded previous complete native inventory per live backend. Within
that lineage native SST filenames are immutable and not reused; compare names
and sizes against that inventory. Hash and upload only new SSTs; do not reread
every retained SST to rediscover identity. Private metadata is uploaded every
checkpoint. On restoration, seed the inventory from the verified selected file
mapping; a new backend incarnation cannot inherit an unrelated local cache.
If an expected reusable object is missing, fail the checkpoint precisely rather
than publish an incomplete inventory. Do not resurrect an old swept path.

## Native and portable restore

Add an optional validated native restore request to configured backend
construction. TableManager exposes the protocol-selected descriptor and storage
context before backend creation; update the five native operator construction
sites without duplicating SQL execution. Validate the descriptor against the
selected source layout/config roster before reading files. Source generation
and fresh attempt generation are distinct.

For RocksDB destination, acquire existing database/cleanup resources, download
verified files into an exclusively created fresh attempt directory, generate
fresh local ownership and open with `create_if_missing(false)`, current shared
cache/WBM/options and Checkpoint durability. Bind registered logical handles to
the restored roster without `ensure_empty` scans or per-row replay. Do not call
diagnostic `reopen`, restore source OWNERSHIP, or open an unmanaged second DB.
On error or cancellation remove only the new attempt/staging data. Existing
source-position, watermark and pending commit restore is untouched.

Preserve current memory↔RocksDB recovery tests. Memory-source logical snapshots
retain bounded import into RocksDB. Native-source recovery to memory uses one
managed temporary RocksDB reader and bounded namespace pages into a bounded
memory restore builder, then closes the temporary DB before acquiring/transferring
the single database permit to the final memory backend. The builder enforces
the normal resident limit and existing decoded/scan/write buffer reservations;
it is not an additional opened database or a full-state cache beyond the memory
destination's actual state. Cover `max_open_databases=1`, failure and cancellation
to prevent circular permit waits or hidden limit increases. Same-backend native
restore must perform zero logical row enumeration.

## Shared retention

Implement the same reachability contract in controller and leader cleanup.
Freeze the canonical head/recovery frontier and prune cutoff before enumeration.
Mark the union of every retained full native descriptor, selected recovery base
and canonical published root awaiting commit completion. Sweep only unmarked
objects born before the cutoff whose owners are no longer publishable. Ordinary
checkpoint/cleanup serialization helps, but generation fencing and recovery-root
protection are required across coordinator replacement. Uncertain roots stop
deletion. Abandoned-upload cleanup uses the same exclusions.

No resurrection: future checkpoints may inherit objects from their confirmed
head/recovery base or allocate new paths beyond the sweep cutoff. They cannot
introduce arbitrary references to old unretained objects. Verify inherited
membership against the authoritative parent/base during merge/publication.
Keep current conditional-create publication, epoch claims, ready proofs, closure
slots and Kafka commit authorization. No distributed refcount protocol.

Read retained manifests one at a time and enforce manifest/file bounds;
do not accumulate historical manifests or full object listings. Use an owned
disk-spooled sort/merge for reachability: 1 MiB accounted sort runs (including
record/container allocations), maximum 16-way merge with 64 KiB buffers, maximum
4 KiB path records. Sort retained references and prune candidates separately,
deduplicate and merge-join to choose deletions. Cap live scratch at 4 GiB, input
records at 16 million and all input descriptor bytes at 2 GiB per pass. Count
every run and intermediate toward the live scratch cap and delete consumed runs
after durable successor creation. Use an exclusively created scratch directory
under the configured local state directory, or the host temporary directory for
controller-only execution, with the configured disk reserve checked before
allocation/growth. Tests use smaller injected limits to exercise multiple passes.
Return a structured cleanup resource error on quota/record/disk/root failure
before deleting payloads; preserve manifests and retry-discoverable candidates.
Keep existing checkpoint-versus-cleanup gating so failed required cleanup cannot
silently allow unlimited new checkpoint files. Never treat a space check as a
global disk reservation against concurrent compaction; test ENOSPC explicitly.
Keep prune candidates retry-discoverable: delete eligible payloads before the
last manifest that records them. A manifest may disappear while shared objects
remain referenced by another full manifest. Never remove a shared directory
recursively. Cancellation removes only exclusively owned unfinished uploads;
late uploads stay fenced GC candidates, never reusable state.

## Resources and scans

Reuse worker cache, memtable, decoded/scan/queued buffers, database, snapshot,
blocking and cleanup pools. Cap in-flight native captures at existing one per
operator. Define transfer chunks at 64 KiB, sequential file transfer initially,
and bound provider multipart buffering explicitly; no unbounded uploader pool.
Use existing disk reserve checks for flush, downloads and pinned staging.
Hardlinks pin obsolete SST bytes during compaction, so measure and bound staging
lifetime and account for disk growth during slow upload/cancellation.

Same-backend checkpoint/restore adds file enumeration, not RocksDB key scans.
Native→memory portability needs full namespace enumeration because every current
key must be materialized in the destination. Use existing namespace lower/upper
bounds and bounded snapshot cursors, with no extra point-read workaround. Report
its frequency, progress across deletions/recovery, snapshot/buffer lifetimes and
actual internal traversal evidence when exercised. Do not add hot-path scans
for telemetry. Distinguish native SST bytes from logical payload bytes.

## Correctness and performance gates

Dev/test matching-prebuilt checks first, using `scripts/cargo-dev`, strict
`scripts/preflight-clippy.sh` and the shared `scripts/rust-build` reservation.
Record profile/native mode and free disk before expensive runs. Reuse the one
shared target; preserve source manifests, hashes, pinned executables and evidence
outside caches. Independent implementation and raw-evidence reviews are required.

Tests must exercise actual generic native operators and both checkpoint modes:
unchanged/changed/retained-unchanged/deleted/reinserted/empty epochs; post-barrier
writes; flush with WAL disabled; compaction; fresh owner/native continuation;
both backend directions; corrupt/missing/oversized/foreign files; partial uploads,
applied write/error, cancellation and retry; stale generations and closure races;
pending-commit replay; removal of obsolete manifests while shared files survive,
final-reference cleanup, late old-worker uploads and interrupted GC retry.

Reuse/extend `test-native-state-tables.py` and lifecycle Rust fixtures for exact
fresh-subprocess epochs and cross-backend behavior; `test-state-table-capacity.py`
for real native MERGE/lookup and all-key beyond-RAM checks;
`test-native-process-recovery.py` for actual worker/coordinator process loss,
source/sink prefix and continuation. Same-process codec/backend recovery does not
substitute for native process loss. Give every new runner a finite outer bound.

Freeze a single matched ABBA campaign before optimized measurement: baseline is
the fetched full-export source, candidate is reviewed native source; both release
builds/pins through maintained tooling. Four 120-second processing arms, at most
480 seconds total, no postdeadline drain or automatic repeat. Fixed native
MERGE/lookup SQL, seed then idle/sparse updates/10% delete-reinsert phases,
49,152 independent 8,192-byte values (384 MiB logical payload) against 32 MiB
backend pools, identical CPU/parallelism/checkpoint cadence and RSS envelope.
Each arm has fixed boundaries: seed during seconds 0-60, idle 60-80, sparse
updates 80-100, delete/reinsert churn 100-120. Early seed completion waits idle
until the fixed phase boundary; include all seed CPU, elapsed time and initial
upload costs. If seed is incomplete at 60 seconds, stop that arm and report the
campaign INCONCLUSIVE without consuming further arms or changing the fixture.
Seed throughput is unestablished; do not claim the fixture will complete within
this deadline. Require completed seed and two completed checkpoints per measured phase or report
the missing acceptance as INCONCLUSIVE without shrinking the fixture or extending
the bound. Freeze actual runner controls/oracle/source hashes before execution.
Require observed native compaction/SST rewrite evidence for compaction coverage;
delete/reinsert alone does not establish it. Missing compaction evidence remains
an explicit acceptance gap, without tuning production settings to force it.

Acceptance is exact results/recovery and bounds, zero retained-row scans or full
hashing on native steady checkpoints, reuse of unchanged SST bytes after capture,
and measured checkpoint CPU/upload reduction in unchanged/sparse phases without
concealing initial upload or compaction cost. Report completed DATA/s, committed
output latency, sync pause and async duration distributions, transferred/reused
bytes, CPU, RSS, I/O, local pinned disk and restore time. For this file-source SQL
experiment, publication latency means event completion to authoritative
checkpoint-frontier publication; it is not measured Kafka read_committed delivery.
Kafka transaction semantics remain a separate process-recovery correctness gate.
Full campaign failures
remain failures; no speedup claim from isolated call counts or incomplete arms.

## File ownership and reviews

Before product code, freeze this plan's SHA-256 and obtain three independent
written verdicts on that exact text: semantics/lifecycle, storage/recovery and
performance/reference validity. Resolve blockers and refresh material changes.

After approval, parent owns TableManager/operator call-site integration, ledger,
build queue and final evidence. Native delegate owns `live/native_checkpoint.rs`,
RocksDB/memory backend construction and generic capture/restore interfaces.
Protocol delegate owns RPC schema, authoritative completion propagation,
checkpoint assembly/validation and both GC implementations. Test delegate owns
new dedicated native checkpoint fixtures/runners and oracle extensions, with
shared module declarations coordinated through the parent. Delegates stay on
this single feature and never launch competing builds. Fresh independent source
review follows implementation; no reviewer approves its own implementation.
