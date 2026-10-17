# STR-144: checkpoint-owned Kafka replay journals

## Failure and repair

The deployed native engine at `e5e64cab9bff66ee21d2f39a047b18ee7e0671ac`
rejects Kafka sink checkpoints when the JSON-encoded replay journal exceeds
1 MiB. The separate configured `sink.recovery_max_bytes` budget defaults to
8 MiB of payload, keys and 64 bytes per record. Byte-array JSON expansion makes
these distinct limits. A shorter checkpoint interval is a scheduling target,
not a hard epoch-size bound; the public patch minimum is one second. Raising
the raw budget cannot repair the fixed commit-RPC metadata limit.

Each transactional sink now writes its pending journal to ordinary checkpoint
GlobalKeyValue table `j`, under its subtask-index key. This table does not use
two-phase commit metadata. The existing two-phase table `i` carries only a
version-3 reference with generation, epoch, transaction index, byte length and
SHA256. All new writes use references. The table writer receives the journal
before the checkpoint barrier through its existing FIFO queue, so checkpoint
publication owns durability and normal checkpoint garbage collection owns files.

An overlap check runs before flush or state replacement. The controller permits
one pending checkpoint, and the sink retains its existing pending-producer
guard. Incoming next-epoch records use the separate active journal. Commit loads
the selected checkpoint's journal by known key and validates reference identity,
length, digest, encoded size, partitions and raw record budget before the
existing transaction commit/replay path. Marker, fencing and replay semantics
remain unchanged.

The coordinating agent explicitly requires reading deployed version-2 inline
commit metadata: ten downstream pipelines already have such checkpoints.
Those reads remain bounded by their original 1 MiB metadata cap. This preserves
actual deployed state; it does not add writes of the old representation or
support older unsupported protocol versions. A missing `j` value fails closed
for a new reference.

## Memory and storage bounds

The raw journal budget remains unchanged. JSON is bounded before deserialization
by `5 * recovery_max_bytes + 4096` with saturating arithmetic. For the default
raw budget this is 41,947,136 encoded bytes. References are at most 4096 bytes.

The existing serializer creates one encoded journal vector. GlobalKeyedView
retains that value and encodes a copy into the FIFO writer; the checkpointer
retains the keyed value and copies it into Arrow buffers before Parquet encoding.
These are finite checkpoint copies, not a claim that raw8MiB means8MiB total
RSS. Commit decoding additionally allocates the bounded decoded records.
Existing GlobalKeyValue restoration reads all selected subtask files, so restored
memory can scale with parallelism times the encoded budget. The MVP proof uses
parallelism one and must not imply constant generic memory at arbitrary
parallelism. No new RocksDB scan, global pool, source throttling or application
behavior is introduced.

## Validation record

Fresh fetched base: `e5e64cab9bff66ee21d2f39a047b18ee7e0671ac`.
Worktree: `/home/jason/repos/streamr-worktrees/kafka-checkpoint-journal-20261017`.
Instructions: `AGENTS.md`, `.claude/team.md`, `.claude/build-test.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`; STR-144 read and
in-flight guard CLEAR. Independent pre-code review approved this bounded JSON
route before implementation.

Use the maintained shared queue and Bookworm image
`localhost/streamr-str127-dev:latest` (immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`),
normal dev/test profile, matching precompiled RocksDB archives. The default
`arroyo-dev` and `streamr-prebuilt-dev` images rejected the current native pin
during preflight; no guard was bypassed.

```sh
STREAMR_DEV_IMAGE=localhost/streamr-str127-dev:latest scripts/cargo-dev fmt --all
STREAMR_DEV_IMAGE=localhost/streamr-str127-dev:latest scripts/cargo-dev test --locked -p arroyo-connectors --lib kafka::sink::recovery::tests
STREAMR_DEV_IMAGE=localhost/streamr-str127-dev:latest scripts/cargo-dev check --locked -p arroyo-connectors
STREAMR_DEV_IMAGE=localhost/streamr-str127-dev:latest scripts/preflight-clippy.sh -p arroyo-connectors
scripts/build-pin --root /home/jason/qa-evidence/str144-kafka-journal-20261017/dev-pin --purpose 'STR-144 bounded oversized Kafka checkpoint journal and real process-loss recovery correctness' --ui-assets /home/jason/qa-evidence/str126-qualification-20261017/release-pin/source/webui/dist --image localhost/streamr-str127-dev:latest --expected-source e5e64cab9bff66ee21d2f39a047b18ee7e0671ac
scripts/rust-build python3 /home/jason/qa-evidence/str144-kafka-journal-20261017/run-isolated.py --case /home/jason/qa-evidence/str144-kafka-journal-20261017/runtime3 --pin /home/jason/qa-evidence/str144-kafka-journal-20261017/dev-pin --legacy-pin /home/jason/qa-evidence/str126-qualification-20261017/release-pin --repo /home/jason/repos/streamr-worktrees/kafka-checkpoint-journal-20261017
```

Formatting, locked connector check and strict Clippy passed. Focused recovery
tests: 9 passed, 0 failed, including an encoded journal larger than1MiB, a small
reference, missing/corrupt journals, identity/length/budget failures and deployed
inline decoding. Independent core source and generic proof source reviews
approved; all configured PR/push CI checks passed at implementation commit
`2fce2231230a32d34a3b4fe15b77e6b5f688953b`.

The maintained normal-dev pin passed with precompiled RocksDB. CLI SHA256:
`b8f66928168e21507d6ccf95e756e4477991eb4dd55968e1514f13bbd6dd9faa`.
Its frozen base is `e5e64cab` plus compiled diff SHA256
`02001f5245ba611831d1929ace4e0f7db83ed98e176aba5486b6996ee949b3ce`;
all compiled-source file hashes match the reviewed implementation commit exactly
(`compiled-source-binding.json`). No source or test binary is mounted in the
runtime container. The legacy qualified CLI SHA256 is
`0e6078d2fe6f2a2c4279b4de3d7a8d6b5486b369b47a23a5c26dcd2bee363a36`.

Actual public Kafka/native CLI case `runtime3` passed:

- The old CLI paused before committing version-2 inline checkpoint2. The new
  CLI restored the same persisted catalog/pipeline/job, replayed that specific
  committing epoch and preserved the existing physical committed prefix.
- Checkpoint3 persisted a 1,860,246-byte JSON replay journal for one620,000-byte
  opaque string record. Its configured raw accounting was620,085 bytes, below
  the unchanged8MiB budget. Commit RPC metadata was259 bytes. The proof reads
  only the exact checkpoint's declared `i`/`j` files and checks generation,
  epoch, transaction index, SHA256, length and full record values.
- An actual worker SIGKILL at the before-commit pause forced generation2 to3.
  The replacement restored/replayed that same checkpoint3. All four committed
  output rows matched exactly; physical committed prefixes remained byte
  identical and a genuinely fresh suffix was produced after recovery.
- Owned process cleanup completed; private broker and network deletion exited0.
  The container had a2GiB memory cap. Maximum sampled sum of owned process RSS
  was354,775,040 bytes; this is diagnostic, not a measured peak, generic memory
  qualification or throughput claim. This finite case does not fill the8MiB
  raw budget or repeat the completed full engine capacity campaign.

Failures retained: `runtime1` stopped before resource creation because a local
broker repository alias was unavailable; `runtime2` correctly replayed deployed
inline state but its log assertion expected an unquoted tracing value. Only the
broker alias and log parser were corrected; Rust source and pinned CLI stayed
fixed. Source binary hashing streams, avoiding a1.7GiB allocation in the test
container. Old local sccache hash objects were reclaimed under the exclusive
shared lock when disk crossed the build guard; no source, target executable or
qualification evidence was deleted, and the old cleanup receipt was preserved.

Evidence: `/home/jason/qa-evidence/str144-kafka-journal-20261017/`, including
`runtime3/result.json`, `runtime3/durable-journal.json`, `runtime3/fault.json`,
runtime logs, physical output snapshots, outer cleanup and source pin receipts.
Final independent evidence review and documentation-head CI are in progress.
Tested native-image publication and production rollout remain separate pending
steps owned by the coordinating agent. Old images cannot read the new reference
representation; this repair provides forward restoration of deployed v2 state,
not rollback compatibility after new-format checkpoints are committed.
