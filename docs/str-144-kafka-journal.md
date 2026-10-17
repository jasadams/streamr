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
```

Formatting passed. Focused tests, normal check/strict Clippy, actual oversized
Kafka checkpoint/process-loss proof, deployed-v2 restoration, independent final
review, current-head CI and tested native-image publication are pending.
Evidence: `/home/jason/qa-evidence/str144-kafka-journal-20261017/`.
