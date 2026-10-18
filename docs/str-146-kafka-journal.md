# STR-146 Kafka compact transaction recovery

All 11 selected acceptance cases now pass on the same preserved dev pin: 9 cases
from acceptance 4 and both corrected CDC cases from acceptance 5. The frozen
unpaced workload commits 65/65 records to both sinks; actual worker SIGKILL
recovery preserves physical prefixes and produces fresh suffixes. Final
independent evidence audit passed; PR CI remains pending.

The affected base `b3da9491de39b9f52cb6ce79e132e9cf378bdf0c` failed a frozen
unpaced real-Kafka reproduction: 65 input records, 64-record backlog, native
`repeat(payload,16)` amplification from 16 KiB to 256 KiB payloads (serialized JSON
larger), two transactional outputs, source batch 1, CP 2 s, 8 MiB replay budget each.
Both sinks exhausted the raw journal before the next aligned barrier; only the
initial row committed in each output. Strict physical prefixes were preserved;
the original baseline anonymous volume remains preserved pending provenance.
Sampled RSS 461688832 B is diagnostic, not peak. The CP1 attempt
failed disk preflight before creating resources and is not an executed result.

Frozen executable SHA256:
`b8f66928168e21507d6ccf95e756e4477991eb4dd55968e1514f13bbd6dd9faa`.
Evidence: `/home/jason/qa-evidence/str146-kafka-readiness-20261018/`, specifically
`baseline-cp2-attempt1/{result.json,failure-lines.json,validation.json}`.
The worktree subsequently fast-forwarded documentation/instructions to
`14353046e5085325b66563981be61c96ecfed7b7`; implementation validation must identify
that base plus its actual source receipt. Read AGENTS.md after this update.

## Authorized repair

The user explicitly authorized implementing Flink-style compact prepared
transaction recovery, including a supported native client capability where
missing. [Flink's producer](https://github.com/apache/flink-connector-kafka/blob/main/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/internal/FlinkKafkaInternalProducer.java)
restores producer identity after delivery/partition registration. The bundled
librdkafka 2.3.0 public interface lacks attachment, so Streamr now vendors the exact
previous `rdkafka-sys 4.8.0+2.3.0` source with a source-level commit-only extension.
See [native origin and changes](../vendor/rdkafka-sys/STREAMR.md).

Checkpoint prepare waits for successful deliveries, includes an atomic
checkpoint marker, seals sends and checkpoints only generation/epoch,
transaction slot and producer ID/epoch. Existing 2PC authorizes commit. A fresh
client restores that identity without InitProducerId/fencing, forces actual
EndTxn and prohibits further sends. The original replay payloads, journal table,
encoded reference and `sink.recovery_max_bytes` option are removed. Markers
remain compact acknowledgement-loss proof; no payload replay fallback remains.
No new SQL/UDF/application logic, checkpoint-visible output delay or source
pacing is introduced. Per-sink transactions still commit independently.

Selected pending recovery preserves its producer slot and fences only abandoned
later work in the alternate slot. Authoritative current restore phase propagates
from controller/worker-leader recovery into generic operator context; committing
operators select control only until matching authorized commit completes.
This repairs the startup race where data/ticks/futures could overtake initial
commit. Ready restorations verify the exact marker during startup. The phase is
not inferred from the manifest's immutable historical needs-commit flag.

The default native producer queue is 8 MiB (`queue.buffering.max.kbytes=8192`), with
explicit existing client overrides retained. Native delivery frees that queue;
Streamr bounds retained delivery futures at 1024. Oversized individual output
records fail before an endless QueueFull retry. These bounds cover accounted
producer bytes/future count, not whole-process RSS. Active/pending producers,
serialized batches and native overhead can coexist. Reproduction must disclose
removal of the obsolete epoch-journal option and mapping to existing native
queue configuration; it cannot claim byte-identical configuration.

Compact recovery depends on broker retention of the prepared transaction for
longer than checkpoint/restart downtime. Expired, aborted or fenced transaction
with no exact committed marker fails closed, with no FinishedCommit. The error
does not infer expiration solely from an ambiguous broker response. Old formats,
rollback/rescale and broker data loss are outside this pre-release contract.
See [operational contract](../crates/arroyo-connectors/src/kafka/sink/RECOVERY.md).

## Validation and limits

The final normal-dev executable SHA256 is
`fcc0760eba30741c700b8d5c8c845640e37e4f6e7b21eb497e532c55764e8d5f`.
`compact-dev-pin3/source-manifest.json` binds the reviewed source based on
`14353046`; all 1267 compiled-source hashes match, with 0 mismatches.
`native-investigation/compact-pin3-native-linkage.json` independently binds the
patched native dependency/archive, Cargo executable fingerprint chain, custom
API symbols and six reviewed native source hashes. Earlier pins remain evidence.
Builds used the maintained shared queue, normal dev profile, matching precompiled
RocksDB and the required source rebuild of extended librdkafka.

Maintained gates 7 passed fmt, locked datastream/planner checks and strict Clippy;
2 fanout regressions and all 14 native-capability planner tests passed. Unchanged
Kafka code passed gates 6: 6 connector test functions (8 logical cases, including
three delayed-coordinator scenarios). The operator restore gate passed its
focused test; build-pin's 18 Python self-tests passed. Mock protocol tests cover
API state/timeout behavior; actual Kafka evidence supplies process recovery.

Final runtime receipts are `acceptance4/results.json` (9 passing non-CDC cases)
and `acceptance5/results.json` (2 passing CDC backend cases). Coverage includes
pre-/post-commit worker SIGKILL, orphan alternate-slot fencing, expired prepared
transaction refusal, memory/RocksDB MERGE state, delayed commit and a blocked
broker. Both coordination modes execute; worker-leader crash recovery is not
claimed. CDC preserves one shared native MAX/COUNT/SUM aggregate, both Kafka
outputs and all eight update/retraction phases with checkpoint-stop/resume on
memory and RocksDB. These CDC phases use orderly checkpoint-stop/restore, not
SIGKILL: independent decoding verifies seven same-job restores, source offsets
1 through 8 and all eight checkpoint hashes. CDC RocksDB
ownership/options files are absent, with no independently proven deletion
cause; backend selection is established by config
and the pinned native constructor rather than direct CDC database birth files.
The separate RocksDB state crash case retains direct database evidence. See
`final-cdc-independent-audit.json` and its execution-2 receipt.

The frozen liveness case retains source batch 1, CP 2 s, unpaced input and native
producer queues of 8192 KiB each; retained delivery futures are bounded at 1024.
It removes the obsolete journal setting rather than silently accepting a no-op.
Fault recovery uses the disclosed 120000 ms transaction timeout; liveness and
expiry checks retain 10000 ms. Sampled owned RSS for final frozen liveness was
435458048 B; CDC memory/RocksDB samples were 423301120/426725376 B. These are
diagnostics, not whole-process peak or broader capacity claims.

Auxiliary fixtures use existing CONCAT for finite MERGE amplification and finite
fusion caps. CDC's logical aggregate key limit is 16939, derived from the encoded
16 KiB MAX argument. Its sole final fixture delta changes the global scan pool
from 2097152 to 3978000 bytes: namespace 33 + escaped key 2×16939 + terminator 2
+ value 32768 + tag 1 bounds every legal checkpoint row at 66682 bytes;
40×(66682+32768) yields a checkpoint page of 66682. Existing key/value guards
cover all eight phases, including old/new update state. See
`native-investigation/cdc-checkpoint-budget-proposal.json` and the v4 fixture
manifest. SQL, oracles, producer queues and engine guards are unchanged.

## Preserved failure history

- Baseline CP 2 exhausted both raw journals; CP 1 stopped at disk preflight.
- Acceptance 1: 1 pass/10 pre-recovery harness failures (4 checkpoint identity
  predicates, 6 internal-field SQL projections); all 11 cleanups were verified.
- Acceptance 2: 2 passes/8 failures. Two actual pre-commit/orphan recoveries
  exposed fresh-client coordinator/API-version readiness. Native commit now
  waits through the existing UP monitor only after explicit authorization,
  retaining the API timeout/retry contract and avoiding InitProducerId/fencing.
  Four MERGE admission fixtures and two CDC key bounds were corrected separately.
- Acceptance 3: 9 passes/2 CDC failures exposed upstream Arroyo chaining's stale
  petgraph edge IDs. Both source-bound fanout tests failed before repair (exit
  101, 0 passed/2 failed); current-edge iteration now conserves successors and
  weights. The native shared-view/two-INSERT SQL remains a regression.
- Acceptance 4: 9 passes/2 CDC checkpoint-read failures (34414 required versus
  derived page 19660). The reviewed auxiliary scan-bound correction above,
  without Rust changes or rebuild, passed both cases in acceptance 5.

Original logs, failed pins and receipts remain under
`/home/jason/qa-evidence/str146-kafka-readiness-20261018/`; diagnosis includes
`native-investigation/{fresh-attach-readiness-diagnosis.md,cdc-fanout-diagnosis.md}`
and `fanout-before/result.json`. `final-root-acceptance-audit.json` verifies all 11 cases use the same binary,
inner/outer success and owned process/container/network/volume cleanup. Maximum
sampled owned RSS across these cases was 468774912 B, diagnostic rather than peak.
Cleanup also removed 32 proven owned anonymous volumes, recorded in
`owned-broker-volume-removal.json`; the original baseline volume with incomplete
provenance remains preserved and is not reported cleaned up.

Independent source reviews approved each repair; final source/evidence audit
passed. PR CI remains pending. This is selected native capability/recovery evidence,
not full application parity, production replay, broader retained-state capacity,
worker-leader crash qualification, deployment or release.
