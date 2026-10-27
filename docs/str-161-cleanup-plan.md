# STR-161: remove recurring empty checkpoint-directory work

Status: proposed; product code and builds prohibited until three independent plan reviews.
Base: `597b6326ab82f9c70634fe3e241f0c355f8e72cf`.
Workspace: `/home/jason/repos/streamr-worktrees/str-161-checkpoint-cleanup-20261027`.
Guidance read: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `/home/jason/.claude/agents/feature-implementation-engineer.md`.
`CLAUDE.md` and `docs/CODING_STANDARDS.md` are absent. Current user instructions
supersede historical signing and elapsed-time requirements.

## Established behavior and evidence

Reuse `/home/jason/qa-evidence/str-idle-cpu-20261027/RESULT.md`; do not repeat
production diagnosis. This increment addresses its historical empty-directory
cause only, with no unchanged SESSION export or checkpoint scheduling change.

- `crates/arroyo-storage/src/lib.rs:704`: local construction creates the root
  and uses object_store LocalFileSystem with automatic cleanup disabled.
- `crates/arroyo-storage/src/lib.rs:766`: prefix enumeration delegates to
  object_store and returns qualified keys. `delete_if_present` removes files;
  it does not remove directory ancestors. Existing `delete_directory` is
  nonrecursive and is not the proposed traversal contract.
- `crates/arroyo-state/src/parquet.rs:461`: orphan discovery fully enumerates
  the owning job checkpoints prefix every cleanup. It intentionally retains
  full discovery for late uploads. Candidate deletion still requires exact
  layout, declared operator ownership, older epoch and absence from retained
  file references. Known-file cleanup precedes it; old metadata deletion and
  publication follow successful orphan traversal.
- Locked upstream source: [object_store 0.12.3 local.rs](https://github.com/apache/arrow-rs-object-store/blob/v0.12.3/src/local.rs).
  `new_with_prefix` defaults automatic cleanup false; `delete` optionally
  removes empty ancestors only after a successful file deletion. Therefore
  enabling that option alone cannot recover already empty historical trees.
  `list_with_maybe_offset` recursively follows directories before discarding
  directory entries; its batching does not bound empty-directory work.
  `new_staged_upload` creates a real temporary file and retries parent creation
  on NotFound. Established staging files make an upload directory nonempty.

## Selected minimal repair

Add an explicit **local-only empty-directory prune before the existing orphan
listing**, scoped exactly to the owning job checkpoint prefix. Cloud backends
perform no additional operation. Keep automatic_cleanup disabled: globally
changing every local deletion would broaden this ticket unnecessarily.

The storage helper accepts an object_store `Path`, applies the configured key
once, and resolves below the configured local root using path segments rather
than raw string concatenation. It performs iterative asynchronous postorder
DFS using `tokio::fs::read_dir`, `DirEntry::file_type`, and `remove_dir`. Store
only active directory frames (O(depth), not an inventory of historical paths).
Never collect all entries or launch per-directory tasks. Operate only below the
requested prefix: preserve the prefix itself and all its ancestors, including
the configured root/key. Reject an empty pruning prefix. Check the prefix and
its existing ancestor components for symlinks, and do not follow child symlinks.
Files, symlinks, staging files and unrecognized entries are never unlinked.

Only `remove_dir` can mutate the filesystem; the kernel checks emptiness at
removal. Missing directories and DirectoryNotEmpty races are benign. Other
read/type/remove errors propagate through existing StorageError/StateError
handling, so the metadata publication path cannot silently declare success.
For platforms returning an OS-specific nonempty error, use the actual standard
ErrorKind mapping supported by pinned Rust rather than swallowing all errors.

Add a private `CleanupStore::prune_empty_directories` operation and delegate it
to storage. Invoke it once immediately before `list_prefix` in
`cleanup_abandoned_controller_job_disk_files`. Test wrappers delegate/trace it
and can inject a pruning failure. Leave the existing listing and every file
selection/deletion condition unchanged.

The first pass pays historical traversal once and removes pre-existing empty
subtrees. Every subsequent pass visits only existing nonempty trees plus empty
directories created since the preceding pass. Orphan/metadata deletions happen
after pruning and can leave directories until the next pass; this is deliberate,
so accumulation is bounded by one cleanup interval rather than job lifetime.
Restarting or retrying requires no persistent cursor, cache or directory index.

Tradeoff: each pass enumerates remaining live directory entries twice (prune
and ordinary object listing). The bounded comparison must report this overhead
on a no-history control as well as the expected large-history benefit. This
repairs historical repeated work without replacing object_store listing or
copying its staging/error/key semantics. A combined custom listing is rejected
for this increment because that would duplicate those contracts. This is an
extra filesystem traversal, not a new RocksDB scan.

## Safety, concurrency and cancellation

- Removing an empty directory cannot remove checkpoint bytes, retained
  references, another owner's bytes, staging files or metadata. Empty directory
  names carry no checkpoint recovery authority. Payload deletion remains owned
  exclusively by the established filters.
- A writer creating a file before rmdir makes rmdir fail benignly. A writer
  starting after successful rmdir recreates parents through upstream upload
  logic; a staged/open upload is protected by its staging directory entry.
  Exercise this against the real provider rather than assuming it.
- Full listing still runs on every pass and after pruning. Late upload after
  listing may survive that pass exactly as today and must be found next pass,
  including with a fresh provider. No old-epoch skip or cached empty marker.
- Async DFS cancellation drops directory frames; an individual in-flight Tokio
  filesystem operation can finish, but no detached full-tree task continues.
  Partial pruning is idempotent and never changes files. Retry with fresh
  storage still performs complete discovery before metadata publication.
- Child symlinks are not descended or removed. Existing object_store listing
  semantics remain unchanged, including following symlinks. The local storage
  root is trusted, as in current object_store; protection against adversarial
  concurrent replacement of path components with symlinks would require
  descriptor-relative traversal and is outside this repair. Test static symlink
  boundaries and document that limitation rather than claim a new sandbox.

## Owned paths and implementation sequence

1. `crates/arroyo-storage/src/lib.rs`: helper and focused real-filesystem tests.
   A private adjacent module is acceptable if needed for readability, with no
   new dependency or general garbage-collection service.
2. `crates/arroyo-state/src/parquet.rs`: private seam and one pre-list call.
3. `crates/arroyo-state/src/parquet/disk_cleanup_tests.rs`: delegate seam,
   pruning-failure ordering, historical/repeated/late-upload regressions and
   finite opt-in filesystem comparison.
4. `docs/str-161-validation.md`: exact source-bound checks and comparison.
   Coordinator owns ledger/tracker. No commits, production mutation or deploy.

## Focused acceptance and evidence

- Storage: preexisting deep/wide empty trees disappear; configured root/prefix
  and neighboring job/prefix stay; nonempty files remain byte-identical;
  qualified key applies once; missing prefix is idempotent; symlinked subtree
  and ancestor are not traversed; real errors propagate; staged upload survives
  pruning and completes; upload into a previously pruned subtree succeeds.
- State: extend exact key/byte retention oracle with historical empties;
  verify repeated passes and fresh-provider late uploads, active epochs,
  declared/undeclared owners and retained references remain correct. Verify
  failure before listing preserves metadata/publication order, and partial
  cancellation plus fresh retry is safe. Keep existing fault/retry tests.
- Finite comparison in the real cleanup test executable: prepare 2,000
  historical checkpoint trees x one operator x two table directories, plus
  four populated retained epochs and an unrelated job. Independently assert
  complete object bytes before/after. Record first-pass removal separately;
  compare eight repeated baseline/candidate passes on independently prepared
  identical fixtures, alternating order. Include a zero-history control.
  Use Linux inotify IN_OPEN on selected historical directories to prove
  baseline reopens them each pass and candidate never reopens them after
  removal; use a separate finite strace phase for actual getdents64/openat/
  newfstatat counts if available. Record wall/process CPU and directory counts;
  report setup outside timed intervals. No throughput or production CPU claim.
  Keep fixtures bounded; no engine, Kafka, release build or capacity campaign.
- After reviews and coordinator queue authorization: record purpose=focused
  correctness/filesystem work, profile=dev/test, matching precompiled RocksDB,
  and disk headroom. Run foreground maintained `scripts/cargo-dev check --locked
  -p arroyo-storage -p arroyo-state`, strict `scripts/preflight-clippy.sh` for
  both affected crates, formatting, and affected storage/cleanup tests with
  `--no-fail-fast`. Coordinator serializes commands and preserves exact logs.
- Independent final review must assess preservation of checkpoint ownership,
  reference/retention, late upload and failure ordering; report exact limitations
  and any failed or omitted check. No new SQL or execution mechanism; no
  architectural/user decision required for the proposed scoped repair.
