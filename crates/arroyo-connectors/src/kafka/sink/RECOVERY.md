# Kafka exactly-once checkpoint recovery

Exactly-once requires a preexisting one-partition `sink.recovery_topic` with
`cleanup.policy=compact` only. Preserve its latest markers for every retained
checkpoint and use stable job/operator/topic/subtask identity with unchanged
parallelism. Read output with `read_committed`.

Each checkpoint flushes output and a generation/epoch marker into the same Kafka
transaction, seals further sends, and checkpoints compact producer ID/epoch plus
transaction slot identity. The existing checkpoint coordinator authorizes
commit. There is no payload replay journal, journal table or output-per-epoch
byte budget. `sink.recovery_max_bytes` is removed. Old checkpoint formats are not
supported under the pre-release policy.

The pinned native client extension resumes a prepared transaction without
InitProducerId or epoch fencing. Recovered handles can only complete that
transaction. Startup fences only the alternate abandoned slot. A selected
pending transaction remains untouched until the coordinator sends its matching
commit; generic operator startup gating prevents data/ticks/futures overtaking
that commit. Ready restorations verify the exact selected marker before running.
After recovery's EndTxn attempt, an exact committed marker proves success even
if the acknowledgement was lost. A missing or newer marker fails closed.

Recovery must finish before Kafka expires the prepared transaction. Configure
native `transaction.timeout.ms` for the expected checkpoint/restart duration
within broker limits. Expired/aborted/fenced transactions without an exact
committed marker cannot be recovered from compact metadata; Streamr refuses
completion. The same is true after broker transaction-state or marker loss.
Arbitrary old-checkpoint rollback, rescale and cross-sink atomic visibility are
not promised. Sinks/subtasks commit independently.

Producer queues use normal librdkafka delivery backpressure. Default
`queue.buffering.max.kbytes` is8192 per producer; explicit existing client config
may override it. The native bound covers accounted queued message bytes, not
whole-process RSS or all allocator/batch overhead. At most1024 delivery futures
are retained before awaiting delivery. A record larger than the entire configured
producer byte queue is rejected rather than waiting forever for QueueFull.
Active and pending producer transactions, one serialized record/batch and native
protocol buffers may coexist. Checkpoint output volume no longer accumulates
serialized records in Streamr memory.

The maintained dev build compiles the vendored native client extension from its
exact source; RocksDB continues to use the matching precompiled dev archive.
See `vendor/rdkafka-sys/STREAMR.md` for origin/checksum and changed native files.
Current validation and limitations belong to `docs/str-146-kafka-journal.md`.
