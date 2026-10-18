# STR-32 Kafka recovery architecture review

Historical source-bound review. STR-146 explicitly replaces payload replay with
compact prepared transaction recovery; see [current design/status](str-146-kafka-journal.md).

Independent read-only review, 2026-10-11. Reviewed engine base `682bea0d0955be2e021fe475b25228f3dacf7d5f`. No engine implementation, configuration, restart, build or fault run was performed for this review. Concurrent user-authorized queue/helper removals are separate work and do not change the Kafka files reviewed here.

## Verdict

The replay journal and atomic Kafka marker solve a real missing upstream capability. They are not merely a way to make a failed test pass. However, the implementation is a **small, fixed-parallelism, latest-checkpoint recovery protocol**, not a generally qualified production Kafka sink. Its output-per-checkpoint envelope is especially restrictive: an 8 MiB raw journal feeds a hard 1 MiB encoded commit-metadata limit. The retained live failure demonstrates that an ordinary backlog can exceed that limit before a checkpoint completes. Restarting the same workload can repeat the failure.

I found no established loss/duplication bug in the reviewed replay-or-skip decision under its declared assumptions. That is a source-review conclusion, not proof that every crash boundary is qualified. Keep the existing protocol unless a separately agreed replacement is justified. Do not declare live/backfill readiness, arbitrary rollback, rescaling, global fanout atomicity, or whole-process bounded memory from the present evidence.

## Why upstream was insufficient

In the parent of recovery commit `156132c0d293e677210f7cd04e151941e9152091`, `kafka/sink/mod.rs:344–364` required an in-memory `producer_to_complete`. After process loss that producer was absent: the implementation logged that restoring from commit phase was unimplemented and returned before sending `FinishedCommit`. It could not finish the checkpoint's pending Kafka commit. This is not evidence of a false successful commit acknowledgement; it is an absent recovery path.

The [public librdkafka transaction API](https://github.com/confluentinc/librdkafka/blob/v2.3.0/src/rdkafka.h) does not expose attachment to another process's unfinished producer transaction. Initializing a producer with the same transactional identity fences old producers and resolves stale transaction state; it does not recreate their in-memory handle. A durable way to distinguish “broker committed, acknowledgement lost” from “transaction aborted” is therefore necessary if Streamr retains its checkpoint coordination. The chosen answer stores replayable records in the checkpoint and a commit marker in the same Kafka transaction as those records.

History shows an initial replay/marker implementation (`156132c0`), bounded two-slot transaction identities (`8ef8e039`), admission before copying (`3f682dea`), encoded metadata cap (`4f4cd244`), and authenticated marker inspection (`96b1cde3`). These are real refinements, but their accumulation does not itself establish an end-to-end architecture acceptance gate.

## Checkpoint and recovery boundary

The source owns its Kafka progress in global checkpoint table `k`; offsets are the **next** offset to read. `source/mod.rs:103–162` assigns restored partitions and seeds their saved progress, including an idle run. `:240–269` advances to `message.offset()+1` and checkpoints that map. Kafka consumer-group commits are auxiliary progress, not the authoritative Streamr restore boundary. Sources and operators must restore the same selected checkpoint as the sink. A consumer configured with `read_uncommitted` can observe aborted output and is outside the exactly-once consumer contract.

The sink's global table `i` contains generation, checkpoint epoch and transaction index. Its committing data contains the original payload, key, timestamp and delivered output partition. At `sink/mod.rs:437–503`, the barrier flushes deliveries, records actual partitions, seals this journal, adds an epoch marker to the same transaction, saves checkpoint state/commit data, retains the old producer for commit and opens the alternate transaction for subsequent work. Only one pending checkpoint transaction is admitted.

Existing controller scheduling loads committing data when the selected checkpoint needs commits (`controller/states/scheduling.rs:343–424`). Worker-leader coordination authorizes the existing durable checkpoint before distributing committing data (`worker/job_controller/model.rs:545–592`). This repair reuses those mechanisms; it does not replace source checkpoints or general checkpoint publication.

At `sink/mod.rs:507–589`, an ordinary process commits its retained producer. A fresh process initializes/fences the checkpoint's producer identity and reads the marker with `read_committed`. If the marker is older than the pending epoch, it republishes the exact saved records to their saved partitions and commits records plus marker atomically. If the marker equals the pending epoch, it skips replay. `FinishedCommit` is sent only after this succeeds. Failure leaves the checkpoint unacknowledged.

Consequently, a crash before broker commit should replay after fencing the abandoned transaction; a crash after broker commit but before acknowledgement should skip. Repeated recovery should make the same decision safely. These are protocol invariants requiring actual broker fault tests, not conclusions from serializer tests alone.

## Ownership, fencing and operational assumptions

- Transaction identities include job/operator/output topic/subtask and alternate two slots (`sink/mod.rs:142–170`). Startup fences both the pending and subsequent abandoned slots (`:353–361`). This bounds producer-identity growth and prevents an older task from successfully publishing through those identities. It assumes stable identity and fixed sink parallelism. Source reassignment code does not establish sink rescale recovery.
- A checkpointed UUID generation and an epoch-zero sentinel distinguish the current job generation from missing/replaced marker history. Marker keys identify the same owner. `recovery.rs:238–290` scans from the retained low watermark to a captured high watermark, rejects a missing/tombstoned/wrong-generation marker and fails if it cannot reach that frontier. Control and aborted offsets are positions, not application record counts.
- `recovery.rs:71–76` rejects a marker newer than the selected checkpoint. This deliberately refuses rollback after externally visible output; it does not undo Kafka output or offer arbitrary old-checkpoint recovery.
- The recovery topic must already exist, have one partition and `cleanup.policy=compact` only (`recovery.rs:154–188`). Replication, min ISR, permissions and preservation of its latest keyed records remain deployment responsibilities. Compaction is compatible with retaining the latest marker; deletion, tombstones, topic replacement and arbitrary broker data loss are not recoverable by this mechanism.
- Records and marker are atomic within one sink/subtask transaction. Multiple sinks/subtasks commit separately; a reader may observe partial fanout while a checkpoint is completing. No cross-sink atomic visibility guarantee follows from eventual checkpoint completion.
- A shared single-partition marker topic can be held behind another producer's open transaction. The scan then times out and fails closed. A dedicated logical-sink topic reduces this interference but parallel subtasks still require qualification. Full retained-log scanning also has a fixed 30-second deadline; large uncompacted history can impair availability.
- Initializing/committing producers uses synchronous 10-second calls with up to six attempts and thread sleeps (`recovery.rs:136–152`). These calls are not cooperatively cancellation-responsive. `on_close` flushes, but does not explicitly abort active transactions. Fencing or broker transaction expiry resolves abandoned work. This is a shutdown/liveness limitation, not a demonstrated duplicate-output defect.
- Replay preserves serialized bytes and actual delivered partition, avoiding re-execution of SQL, random IDs or partition selection during pending-commit recovery. Changes to output partition topology or serialization configuration are not qualified. The inherited nullable key extraction uses `value(i)` without a null check (`sink/mod.rs:430`); replay preserves the key actually published, but nullable-key behavior deserves its own contract test before claiming SQL null fidelity.

## Resource contract and the live failure

`recovery.rs:17–21,79–134` imposes two different bounds: payload + key + 64 bytes per record must fit `sink.recovery_max_bytes` (default 8,388,608), and the full JSON journal must fit 1,048,576 encoded bytes. Raw admission happens before journal cloning/publication. Encoded admission occurs after allocating JSON, before checkpoint publication. JSON byte arrays expand substantially; pending and active data, serialization allocations, delivery futures and librdkafka buffers can coexist. These limits do not establish a whole-worker RSS bound.

The 1 MiB cap protects the qualified one-subtask/one-sink path from the default 4 MiB commit transport frame. It does **not** preflight the sum of journals copied into a multi-sink/multi-subtask `CommitReq`. Such a frame can exceed the transport limit even when each individual journal is legal. This is an unqualified composition risk, not an observed corruption in this review.

The independently retained [live diagnosis](/home/jason/qa-evidence/str32-post-merge-20261011/live-journal-diagnosis.md) decoded checkpoint 3196's source partition 0 next offset as 116335. A later reported high watermark was 160868: a distance of 44533 offset positions, not a record count. The live encoded-journal failure establishes a real availability gap. Increasing the raw journal budget alone cannot lift the encoded cap; shorter checkpoint intervals are mitigation, not a guarantee during fast catch-up or delayed checkpoint completion. The current Kafka SQL constructor supplies no caller-controlled catch-up rate limit. Removing graph queue admission does not remove this independent sink limit.

There is no safe configuration-only guarantee for arbitrary catch-up under this protocol. A bounded deployment may declare and measure output-per-checkpoint limits, but automatic restart can otherwise encounter the same failure again. This review did not reset offsets, discard journals, change configuration or restart the failed pipeline.

## Evidence actually available

Current `recovery.rs` has seven helper tests covering budget/overflow, record wire round-trip, state validation, terminal transaction retry, rollback rejection, legacy rejection and oversized encoded metadata. The Kafka sink integration fixture in `sink/test.rs` initializes **at-least-once** mode; its flushing test does not qualify this exactly-once recovery protocol. Opt-in before/after commit pause hooks exist (`recovery.rs:306–323`), but hooks are not fault results.

The retained fresh-runtime result at `/home/jason/qa-evidence/str32-20261010-52391450/kafka-idle-offset-repair/fresh-runtime-20261010/result.json` reports exact initial/idle-restart/continuation scans: 14 capture, 14 unified and 2 merge records. Its stated scope explicitly excludes arbitrary crashes. A separate stopped live-prefix audit checked 1389 events. These support particular application/source continuity cases, not all crash boundaries, multi-subtask transactions, aggregate RPC frames, retention loss, controller/broker faults or catch-up capacity. No source-bound STR-37 before/after broker-commit crash receipts were located during this review; their status is **unverified here**, not asserted failed or passed.

## Prioritized decisions and MVP actions

1. Keep the public claim narrow and retain failed live evidence. If MVP requires unattended Kafka catch-up, treat the demonstrated journal limit as an acceptance blocker for that use case. It need not block a deliberately bounded demonstration. Decide the required workload before choosing a repair.
2. Qualify the existing protocol with fresh-process kills before broker commit and after broker commit before acknowledgement; repeat recovery, include an empty checkpoint, compare complete physical `read_committed` output and source/state continuation. Bind receipts to source and binary. Add marker loss/newer-checkpoint refusal, fencing and cancellation cases. These tests validate the chosen contract without architectural expansion.
3. Establish aggregate commit-frame and memory bounds for the supported sink/subtask count. Measure output volume during the intended backfill/live workload. Do not equate a per-journal cap with global admission or raise limits until failures disappear.
4. If general catch-up is required, discuss a durable replay location referenced by compact checkpoint metadata, or an explicit output-aware checkpoint/backpressure contract. The former removes journal payloads from RPC but adds storage ownership/cleanup/recovery obligations; the latter must preserve barrier progress and cannot simply block a sink forever waiting for a barrier behind blocked data. Neither is implemented or approved by this review.
5. Alternatives change the promise: at-least-once plus caller deduplication avoids this transaction recovery protocol but permits duplicates; Kafka-native consume/produce transactions can atomically commit Kafka offsets but do not automatically coordinate arbitrary Streamr operator state or external sinks. An attach/resume transaction API would require a different supported client capability, not an assumed librdkafka operation.

None of these alternatives is automatically required for every MVP. No architectural redesign, output semantic change or new capability is approved by this document.

## Reviewed source receipts

| File (under `crates/arroyo-connectors/src/kafka/`) | SHA-256 |
| --- | --- |
| `source/mod.rs` | `691bd425e7c6905f1d16d32644cfd2cf1899277d231017ff81fab233d0fb4f28` |
| `sink/mod.rs` | `1da3232f2155a1479823c75bbbc1c5a347e8df6817b9a19d8d33d2fa57a8db13` |
| `sink/recovery.rs` | `8cc726001ddcff8695f24a433ea4eb7105ca697955cf1e30ce6fb5b01d3d845c` |
| `sink/test.rs` | `bcc2596cb2fc0d2287e06578481d43f16c022d8472f9f91aa0ea9d48709ce0dd` |
| `sink/RECOVERY.md` | `21ddaae64e22020885aa687f1076f7647351408102f464837c2f1856871d0987` |

Applicable instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, `docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`. Written verdict: **the recovery mechanism is justified under a narrow contract; broader production Kafka qualification remains pending, and the observed catch-up availability gap is real.**
