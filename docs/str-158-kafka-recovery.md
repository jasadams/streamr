# STR-158 prepared Kafka transaction investigation

Investigation base: `597b6326ab82f9c70634fe3e241f0c355f8e72cf`.
Instructions read: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, STR-158 full ticket,
`docs/str-146-kafka-journal.md` and the canonical backlog-fast workflow.
No product changes or production actions are authorized by this report.

## Existing contract and source diagnosis

The STR-146 compact protocol saves generation/checkpoint/transaction index and
producer ID/epoch, not output payloads. Output and the checkpoint marker share
one Kafka transaction. The global checkpoint authorizes EndTxn only after all
operators report checkpoint completion. Pending restore attaches the saved
identity without InitProducerId; only the alternate abandoned slot is fenced.
An exact read-committed marker can establish success after lost acknowledgement.
A previous marker cannot establish success of the selected checkpoint.

`kafka/sink/mod.rs` commit distinguishes original producer from recovered client.
Original EndTxn errors propagate directly; recovered EndTxn errors are followed
by bounded marker scanning. `retry_transaction` retries only retriable errors
(up to six 10-second API attempts), not fatal fencing. The broker transaction
lifetime is independent of that API timeout. In bundled librdkafka 2.3.0,
`rdkafka_txn_normalize_err` maps INVALID_PRODUCER_EPOCH and PRODUCER_FENCED to
_FENCED; EndTxn classifies both as fatal. The error wording alone does not prove
a second producer was started, nor does it establish historical expiration. The pinned Redpanda v24.3.7
`tx_gateway_frontend.cc` internal-abort completion explicitly requests producer
epoch bump to fence an expired producer. Apache Kafka3.9
`TransactionCoordinator.abortTimedOutTransactions` likewise prepares a fenced
epoch before coordinator ABORT. Raw primary source and hashes are retained in
`upstream-source-receipts.json` under the evidence root.

[Kafka producer configuration](https://kafka.apache.org/40/configuration/producer-configs/)
and [librdkafka configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
describe coordinator abort after transaction timeout; caller API waits cannot
extend an already expired broker transaction. [librdkafka transaction handling](https://github.com/confluentinc/librdkafka/blob/v2.3.0/INTRODUCTION.md)
distinguishes retriable, abortable and fatal outcomes. [Upstream Arroyo sink](https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-connectors/src/kafka/sink/mod.rs)
uses checkpoint-directed transactions; Streamr's compact prepared attachment and
exact marker recovery are fork additions, documented in STR-146.

Production evidence is preserved at
`/home/jason/qa-evidence/str158-identity-kafka-recovery-20261026/`.
Identity pending4457 versus marker4456, global checkpoint61.816s, prepared sink
wait59.45s and configured broker timeout60s justify investigating expiration;
they do not prove the original historical cause. Profiles1017/1016 is a second
observed case. No repeat production probe campaign is needed.

## Frozen finite causal experiment

Reuse the STR-146 frozen65-record declared fixture and its two-output native SQL
amplification graph. This fault case actually publishes two inputs (one prefix,
one fault record), consumes those two inputs, and withholds the other63 records;
source batch1, checkpoint2s, RocksDB, native queue8192KiB and unchanged transaction
timeout10000ms. After one committed prefix and orderly checkpoint stop, preload
one additional input and hold existing debug before-EndTxn gate. Decode durable
committing checkpoint, selected source next offset, both saved PID/epoch/index
and broker descriptors. Require actual CompleteAbort for both identities before
release. Release while original worker is alive, capture original non-recovery
EndTxn error for a selected sink, then SIGKILL the exact owned worker. Require
same pending checkpoint restore and explicit missing-exact-marker refusal.
Capture literal source/output/marker artifacts, physical committed prefix and
prior-only marker equality; withhold the fresh suffix. A refusal case cannot
claim successful suffix continuity.

This gate is after global committing authorization. It isolates transaction age
and original EndTxn failure, but does **not** reproduce a slow operator's async
checkpoint before global completion. Existing hooks are before/after EndTxn and
commit dispatch; none delay an operator before checkpoint completion. SIGSTOP
of the single worker freezes sinks too. A deterministic operator-duration case
would need a separately approved debug-only asynchronous gate in
`arroyo-state/src/tables/table_manager.rs`, immediately before sending
`ControlResp::CheckpointCompleted`, selecting job/operator/epoch and emitting
PID/timing receipts. That leaves other operator tasks/sinks runnable without
changing production behavior. After the first causal pass, the coordinator authorized the debug-only hook
described below; this is fault-injection support, not a recovery repair.

## Execution evidence and remaining checks

Evidence root: `/home/jason/qa-evidence/str158-reproduction-20261027/`.
Python source compilation checks passed for both changed harness scripts.
The maintained outer queue build/pin/run command exited1 before starting any
build: automatic cache cleanup could not meet machine disk reserve/budget.
It reported free82.6GiB before and96.7GiB after cleanup, Cargo caches56.2GiB.
After coordinator-audited removal of regenerable compiler cache only, the retry
passed build-pin and the finite case in one reservation (`command-0.json` and
`command-1.json`, exit0). Cargo dev build completed in17m43s, matching prebuilt
RocksDB and immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Executable SHA256:
`8ea202e5472db0cb93be78c7936142f805f85decad9a827018547c2935638b04`.
`dev-pin/source-manifest.json` retains compiled source/native/linkage receipts.
Sink and vendored native protocol source is unchanged from production source
476baa5 (`production-protocol-source-comparison.json`).

`original-expired-1` passed the frozen refusal experiment. Job `job_jMFWQRCCfB`,
checkpoint3, both transaction indices3, saved producer IDs4/3 and epochs3.
Both exact broker descriptors were Ongoing with timeout10000ms, then
CompleteAbort at epochs4 before original EndTxn. Broker source contained exactly
inputs0/1; selected source next offset2. Only input0 remained committed in each
output. Exact marker keys/generations still ended at checkpoint2.
At08:46:54.096678Z, the original sink0 producer failed EndTxn with fencing.
The harness captured that original error before any compact resume and sent
pidfd-bound SIGKILL to original workerPID196/generation2. ReplacementPID809/
generation3 restored checkpoint3; both saved identities were resumed and both
refused observed marker2/expected3. Physical output prefix and committed marker
values remained unchanged.63 fresh input records were withheld; successful fresh
suffix recovery is not claimed. Inner/outer exit0; owned worker/container/network/
anonymous-volume cleanup passed. Sampled owned RSS399728640B is diagnostic,
not a peak or capacity result.

This proves that broker transaction expiration alone can fence the **original**
producer and make compact recovery fail closed, without a competing producer
for the selected transactional ID. Only sink0 original failure was captured
before SIGKILL; both recovered sink failures were captured. It does not prove the historical production
cause and does not yet exercise actual global operator checkpoint completion
across the transaction window. The external `operator-completion-design.md`
describes the narrow debug-only test instrumentation and meaningful focused
tests needed for that next causal case. The coordinator subsequently authorized that narrow instrumentation. A private
`checkpoint_completion_fault` module is compiled only with debug assertions,
requires an explicit test directory, hex-encodes exact job/operator/subtask/epoch
scope, consumes one request using an atomic no-overwrite claim, yields while
withholding completion, and fails loudly after30s without sending completion.
The hook sits immediately before `CheckpointCompleted`; exported metadata's
finish_time remains earlier and is not used as a state-export duration claim.
Four focused tests cover exact scope/one-shot, cooperative progress/release,
deadline error and requestless behavior. Source review approved; all four tests
passed with `--no-fail-fast` (94 unrelated tests filtered).

The `checkpoint-expired` scenario targets the same frozen graph's checkpointed
source operator, avoiding a fixture/SQL/budget change. It requires dual prepared
sinks, broker Ongoing then CompleteAbort while the catalog remains inprogress
and source completion absent. It retains at least10.25s of externally observed
withholding before release, then requires normal committing and the same original
failure/process-loss/restore oracle. `checkpoint-expired-1` passed with all
seven maintained command exits0 (`completion-command-0.json` through6): fmt
apply/check, locked state check, strict all-features/all-targets state Clippy,
four focused tests, normal-dev build-pin and the finite real-Kafka case. Build
time10m48s; test build4m41s. The developer image was selected through supported
`STREAMR_DEV_IMAGE` after the stale default image refused prebuilt RocksDB;
failed preflights and image-attempt receipts are retained.

The new executable SHA256 is
`63e4f767127f1978985c72ed3b63e626cff053ee0f13c04be4047e8c17b4d7fc`.
Job `job_xSIG0qYQ4U` checkpoint3 withheld actual source operator completion in
workerPID203/generation2. Both same-epoch sinks were prepared while the catalog
remained inprogress and source completion absent; exact broker IDs3/4, epochs3
were Ongoing then CompleteAbort at epochs4 before release. External observed
withholding was10.283804772s; the actual controller CheckpointingOperators span
was10.760966s, exceeding unchanged10000ms transaction lifetime. The exported
source metadata timestamp predates withholding and is not an export-I/O metric.

After release, normal durable committing preceded original sink1 EndTxn fencing
at09:18:33.401033Z, captured before actual PID203 SIGKILL. ReplacementPID968/
generation3 restored checkpoint3. `selected-restore-refusal.json` captures the
first exact selected sink refusal; the raw `runtime.log` contains both selected
PID/epoch/index resume and marker2/expected3 failure pairs for independent audit.
Literal broker source again contains only inputs0/1, saved next offset2; exact
1-row physical output prefixes and prior markers remain unchanged,63 inputs
withheld. Inner/outer exit0 and owned process/container/network/volume cleanup
passed. Sampled owned RSS401563648B is diagnostic, not peak or capacity evidence.

Both cases retain actual runtime supervisor/wrapper/helper source files matching
their recorded SHA256. The first supervisor was recovered by reversing only
second-case additions and accepted only after byte-for-byte hash equality with
the original run receipt. The final pin binds compiled Rust; the outer runtime
receipt separately binds actual Python harness hashes. No new RocksDB scan was
introduced. The hook is compiled only with `debug_assertions` and is inert without the
explicit test environment; the normal release profile omits it. No release-
profile build was needed or performed.

No repair is selected. The smallest preventive option already exists:
`sink client_configs='transaction.timeout.ms=...'` can select a broker-supported
lifetime that covers the full open transaction, including checkpoint alignment,
operator completion and restart. Lifetime begins before prepare; a prepared wait
below60s does not establish that the full transaction stayed within60s. The fixed
EndTxn API wait is a separate setting and extending it cannot revive an aborted
transaction. No new configuration or protocol is proposed from this test alone.

A preventive timeout/latency repair cannot reconstruct
already aborted prepared output. New rewind/replay, force acknowledgement,
skipping markers or resetting state would change the contract and require user
agreement. Historical production cause and a continuity repair remain open. Actual operator
checkpoint-duration reproduction is now demonstrated through the disclosed
synthetic completion withholding, not slow serialization or state-export I/O.
Independent first-case raw evidence review passed (`review-first-case.md`), and
instrumentation source review passed (`review-completion-source.md`). Final
source/raw evidence review PASS (`review-final.md`,
`independent-completion-audit.json`); publication/current-head CI remain pending.

Any restore-earlier/replay recovery contract requires explicit user agreement
and per-sink commit/duplicate protections before implementation. Existing timeout
configuration can prevent future expiration within its configured operating
window; it cannot reconstruct a prepared transaction that has already been aborted.
No safe recovery of selected production4457/1017 has been established; their
historical abort lineage is not proven. No contract-preserving recovery defect
has been demonstrated and no continuity repair was implemented. No production action, recovery redesign, replay, reset, missing-
marker suppression or force acknowledgement was performed. No deferrals beyond
STR-158's still-open historical-cause/recovery decision.
