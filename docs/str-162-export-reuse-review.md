# Unchanged live state checkpoint export review

2026-10-28 user direction supersedes this candidate: pursue the full performant
native RocksDB checkpoint solution rather than Parquet-copy reuse. The copy
proposal below is investigation history, not selected implementation scope.
See [native checkpoint direction](str-25-native-checkpoint-direction.md).

The original STR-162 review considered avoiding repeated scans and Parquet
compression when live state has not changed. Direct references to an earlier
epoch's files were unsafe under the reviewed base protocol. The smallest
candidate preserved a complete, independently owned snapshot in every epoch
by copying an unchanged complete export to fresh
paths. This requires a trustworthy mutation token and a bounded copy operation;
neither existed in the reviewed base interface. This copy candidate was not
selected; user agreement instead authorized the native implementation linked
above. The reference analysis below describes the pre-implementation base.

## Source and scope

Reviewed base: `8cddb321b631b79185bf6d166a0fdf9522db529f`, fetched `origin/main`
on 2026-10-28. Worktree:
`/home/jason/repos/streamr-worktrees/str-162-export-reuse-20261028`.
Guidance: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, and the full current Trakkt STR-162 description and
comments. Explicit user instructions retire signing and automatic time limits.
The invoking checkout and its unrelated changes are preserved.

Existing diagnosis is reused from
`/home/jason/qa-evidence/str-idle-cpu-20261027/RESULT.md`: SESSION consumed about
0.261 CPU core during stalled DATA at source `597b6326`. Export/scan stacks were
95.57% of eligible samples with 64.74% process CPU coverage; these percentages
do not establish total CPU attribution. Existing burst evidence in
`/home/jason/qa-evidence/arcstream-burst-cadence-20261028/RESULT.md` concerns
changing replay state. Unchanged export reuse is not an established remedy for
that workload. This review adds no production measurement or mutation.

## Existing lifecycle at the reviewed base

| Stage | Reviewed base behavior and source |
| --- | --- |
| Barrier | `tables/table_manager.rs:1045-1121` suspends operator processing, acquires a capture permit, captures one stable snapshot per shared backend and records all registered tables before queuing the checkpoint. |
| Export | `tables/table_manager.rs:197-298` exports every captured disk/typed table. `live/checkpoint.rs:289-537` scans its entire namespace, writes complete immutable compressed files, and records paths, sizes, row counts and checksums. Empty state is explicit. |
| Completion | `tables/table_manager.rs:321-360` builds fresh per-epoch subtask metadata, watermark and configs, checks wire limits and reports completion. This is worker completion, not global publication or Kafka commit. |
| Failure | Export attempts best-effort deletion of its own files on an ordinary error (`live/checkpoint.rs:524-533`). Drop aborts the flusher (`tables/table_manager.rs:406-412`); cancellation/crash leftovers depend on fenced cleanup. |
| Recovery | `tables/table_manager.rs:495-542,862-889,989-1024` checks selected epoch, generation, owner, namespace and config before restoring. `live/checkpoint.rs` validates content/checksums and restores through backend writes. Local WAL is not selected recovery state. |
| Compaction | `parquet.rs:298-299` excludes disk/typed tables from controller Parquet compaction. Local RocksDB compaction does not supply a reusable logical export identity. |

Paths in this table are relative to `crates/arroyo-state/src/` at the reviewed
base. The full snapshot includes internal indexes and maintenance state in the
namespace, not just application-visible values. Source offsets, transaction
commit data, pending outputs and operator progress continue through their
existing tables and fresh per-epoch metadata. An unchanged live table does not
mean an unchanged checkpoint or permit skipping a barrier or Kafka transaction.

The live backend API (`live/mod.rs:288-345`) has stable snapshot readers but no
mutation revision. Cursor snapshot IDs identify a particular reader, not equal
contents. Memory snapshots clone rows (`live/memory.rs:213-225`); RocksDB
snapshots pin a native view (`live/rocks.rs:460` onward). Neither can currently
prove equality without enumerating state.

Pinned upstream Arroyo commit
[`8a87ecca445d6c3af47257a41ddb2e9b0c4c2c28`](https://github.com/ArroyoSystems/arroyo/commit/8a87ecca445d6c3af47257a41ddb2e9b0c4c2c28)
is available in local Git history. Its `tables/table_manager.rs:95-111` already
passes prior checkpoint metadata into table checkpointers while keeping fresh
per-epoch completion/progress. Upstream `expiring_time_key_map.rs:290-293,360,745`
onward carries forward append/expiry files with explicit retention; the current
equivalent is `expiring_time_key_map.rs:293-299,362-371,752-775`. That machinery
demonstrates reuse for a different file lifecycle. Current
`global_keyed_map.rs:216-229,376-435` ignores previous metadata and writes a full
replacement. Neither provides a safe dirty-only replacement shortcut.

## Why old file references were unsafe at the reviewed base

`crates/arroyo-state-protocol/src/disk.rs:20-95` and
`typed_checkpoint.rs` validate epoch/generation ownership of file paths.
`disk.rs:120-239` also checks ownership when validating a manifest. Worker
restore independently checks the selected epoch's table prefix. Reusing old
metadata fails those checks; changing only its epoch leaves foreign paths.

There are two cleanup paths. Controller-owned legacy cleanup loads the retained
minimum's table references and deletes old unretained files
(`arroyo-state/src/parquet.rs:326-435,463-511`). That is not a general union of
all retained checkpoints. Leader cleanup validates each prunable manifest and
deletes the disk/typed files it lists
(`arroyo-state-protocol/src/gc.rs:65-241`). It does not retain a file because a
later checkpoint mentions it. Fenced abandoned-upload cleanup also relies on
generation and epoch ownership (`gc.rs:257-325`).

Relaxing validation alone would therefore introduce data loss on retention.
Cross-epoch references require a new reachability/lifetime contract across
publication, both cleanup paths and recovery. That is beyond this scoped
proposal and overlaps the broader STR-25 checkpoint architecture discussion.
Dirty-only replacement snapshots are also excluded: they omit unchanged keys.

## Historical copy proposal (not selected)

1. **Capture a conservative mutation token with the snapshot.** Use a backend
   incarnation plus a monotonic backend-wide revision. Equality is sufficient
   evidence of no mutation; inequality always takes the full-export path.
   Capture the revision and native snapshot under the same write exclusion.
   Advance or invalidate the token before any write can become visible, including
   admitted writes, deletes and restore writes. A failed or cancelled write may
   invalidate unnecessarily. It must never leave an applied mutation unmarked.
   RocksDB can apply a write and then return a flush-scheduling error
   (`live/rocks.rs:244-281`), so advancing only after the outer call returns
   success is unsafe. Overflow disables reuse; it must not wrap into equality.
   Backend-wide changes conservatively invalidate all tables on that backend,
   avoiding a growing namespace revision map. This deliberately misses reuse
   after equal-value writes or delete/reinsert cycles.
2. **Keep one complete export candidate per registered table.** The candidate
   holds the captured token, namespace, exact config/format, owner/incarnation,
   and the bounded complete file descriptor list. It holds no row cache or native
   snapshot. Replace it only after all files and subtask metadata validation
   succeed. Never label a post-barrier revision as the exported revision. A fresh
   worker starts with no candidate and fully exports its first checkpoint after
   selected-state restore. No recovery-time inference from an old process token.
3. **Create fresh files owned by the current epoch.** With matching token and
   identity, copy every candidate file into unique current-epoch table paths.
   Preserve complete contents, checksums, counts and format; emit a new descriptor
   with the current epoch/generation. No dependency on earlier files survives
   completion. Empty candidates produce a fresh explicit-empty descriptor.
   Do not reuse any unrelated table's metadata, watermark or commit data.
4. **Bound copy work through the storage abstraction.** `arroyo-storage` exposes
   reads, writes and a raw backing store (`lib.rs:820-975`), but has no supported
   checkpoint-copy wrapper. Add a qualified-path, immutable-destination copy
   operation with explicit lifetime, admission, retry and cancellation behavior.
   Enforce declared size, checksum integrity and create-only complete destinations.
   Use bounded streaming or a verified server-side copy; no full-state buffer,
   unbounded parallel copies or raw path bypass. Initial scope excludes a local
   hardlink specialization. A hardlink saves bytes only with additional local
   filesystem and immutability guarantees; it is not a portable storage contract.
5. **Missing source is a cache miss, never an incomplete snapshot.** Retain the
   current captured snapshot until copy succeeds or full export finishes. If a
   source disappears, remove only this attempt's destination files and export
   the entire captured snapshot to new unique paths. Discard the entire partial
   reuse descriptor; never publish a mixture of partial copy and fallback files.
   Verification failure must safely fall back or fail the checkpoint. Other storage errors follow the existing task
   failure policy unless a bounded fallback is explicitly justified. A cancelled
   attempt reports no completion and leaves no reusable candidate from partial
   work. Late finished copies remain owned by the abandoned destination epoch
   for existing GC. Cleanup must never delete a source as part of copy rollback.

Source lifetime needs no new persistent lease if the copy primitive either
finishes from a stable source handle/object version or reports a missing source
and triggers full export. This must be demonstrated against cleanup races for
each supported storage implementation. Do not assume a worker-local completed
export is globally published or retained. If storage cannot provide that copy
contract, disable this optimization there rather than silently add a controller
pin protocol.

This preserves the current full replacement format, validation, retention,
ownership, coordinator and Kafka recovery protocol. It adds an internal
optimization contract to the generic backend/storage boundary. User agreement
is required before implementing it under STR-162 and the repository work rules.

## Benefit and tradeoffs

The hypothesis is lower CPU and RocksDB read work on genuinely unchanged state.
Portable copying still reads/writes checkpoint bytes and creates new epoch files;
it does not reduce retained storage or guarantee shorter checkpoints. Server-side
copy may reduce worker traffic, depending on the backend. A backend-wide token
can miss opportunities when another namespace changes. Write-heavy replay keeps
the full export path, so no replay throughput or committed-visibility improvement
is claimed. No checkpoint interval, memory/cache/resource limit or SQL semantics
changes are proposed.

No new RocksDB scan is proposed. The existing full export scans the complete
namespace because replacement snapshots must contain every key. The unchanged
path would enumerate only the already bounded file descriptor list; known-key
file copies replace state enumeration. It must release native snapshots and
permits promptly after completion/fallback and preserve existing resource limits.

## Implementation acceptance after agreement

- Memory and RocksDB: full export at epoch 1; unchanged epoch 2; changed and
  deleted keys at epoch 3; delete all to explicit empty; unchanged empty epoch;
  mutate again. Decode each selected epoch into a fresh owner and compare exact
  values, indexes and absence of deleted keys. Include a still-unchanged key.
- Post-barrier writes during export/copy, shared-backend unrelated writes,
  no-op writes, backend replacement, revision overflow and applied-write/error
  cases must conservatively preserve the selected snapshot.
  Include multi-table batches across namespaces and concurrent admitted-write
  and snapshot scheduling to exercise the backend-wide atomicity contract.
- Inject copy failure, missing source/retention race, cancellation, upload retry
  and worker loss. Verify no partial completion, isolated destination cleanup,
  bounded candidate lifetime and fresh-worker continuation. Prune source epochs
  through both cleanup paths and restore the retained destination independently.
- Exercise disk and typed tables, legacy/controller and leader publication where
  supported, actual source-position/pending-output/commit recovery, metadata/file
  limits and storage qualification. Preserve existing ownership rejection tests.
- Run maintained dev/test matching-prebuilt checks and strict Clippy for affected
  crates in the shared queue. Preserve exact source/image/config evidence.
- Preregister finite matched alternating unchanged/changing/churn runs. Compare
  completed checkpoints and rows, CPU, wall time, bytes, I/O and RSS with exact
  equality/recovery oracles. Use appropriate explicit optimized builds only for
  optimized performance claims. No new production profile or full capacity
  campaign is required to rediscover the diagnosis.

## Review and decision

Independent storage/recovery (`lifecycle_review`) and mutation/semantics
(`mutation_upstream`) analyses and final integrated document reviews on 2026-10-28
APPROVE this proposal for user decision, with no blocking findings. Both reject
direct old-file references and dirty-only replacement. The semantics review's
nonblocking multi-table/concurrent-write test clarification is included above.
Both reviews conclude:
this exercises the existing native checkpoint capability; it bypasses no SQL
capability; no architecture deviation is authorized by this review. No product
code, builds, recovery tests or performance experiment have run
for STR-162. The decision is whether to implement the bounded complete-copy
candidate, accepting continued checkpoint byte I/O and conservative cache misses,
or leave this idle cost unchanged and discuss broader checkpoint work separately.
