# STR-32 MVP cleanup validation — 2026-10-11

The user-authorized cleanup restores Arroyo row-based graph/network backpressure and default DataFusion execution, removes global operator execution-memory admission and deletes unused timer/ranked/history helpers. Native SQL, state tables, backend resource limits and existing checkpoint/recovery protocols remain. The Kafka architecture review is complete; it does not authorize a protocol redesign.

## Source and delivery

- Fetched base: `682bea0d0955be2e021fe475b25228f3dacf7d5f`.
- Validated implementation: `e4aec059035ca4b03ef05167c8d1a1491ef2ddcd`.
- Worktree: `/home/jason/repos/streamr-str32-arroyo-queues`.
- SQL executable SHA256: `381f04bebcc76351c870542158447a73e180af2591d8e90060f008fde07f8133`.
- Development image: `a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`.
- Independent source reviews approved the cleanup and all material corrections. [PR36](https://github.com/jasadams/streamr/pull/36) publishes this increment; publication does not imply merge or deployment.

Exact source, image, query, input, oracle and executable receipts are retained outside regenerable Cargo caches in `/home/jason/qa-evidence/str32-arroyo-queues-20261011/`. The same executable was reused for all capacity and SQL matrices; no rebuild was needed for the longer timeout or follow-up matrices.

## Required checks

Locked checks for operator/worker/state/RPC/SQL-testing, formatting and strict all-feature/all-target Clippy passed in the required container. Library tests passed: operator8, worker163, state72 and RPC35 (278 total). All21 changed Python harnesses parse. Six active PR CI checks passed on the implementation head; full Rust CI is disabled, so those CI checks do not substitute for these container results.

Existing small SQL matrices passed all48 configurations: aggregate8, updating joins32 and state tables8. They cover memory/RocksDB, controller/leader and source batches1/8, complete typed values, CDC where applicable, checkpoint-prefix output and fresh-worker continuation. Join fixtures retain their existing explicit16MiB scan/64MiB queued-write profile; these small cases are not the62MiB capacity profile. Independent receipt/output audit passed all48 cases, including exact saved checkpoint byte prefixes and nonempty recovered suffixes.

## Single-aggregate capacity and recovery

The100000-key native COUNT/MAX fixture retained a declared819200000-byte logical payload floor at both the proper199999-input checkpoint and final state. Payloads are8192bytes; source batch32; backend pools62MiB; whole-child RSS ceiling512MiB. Initial output100000rows and recovered output200000rows passed full independently generated values and strict CDC before-image comparison. The100000-row saved checkpoint output is an exact recovered byte prefix followed by100000fresh rows.

Whole-child peak RSS was366641152bytes (349.7MiB), below536870912bytes. Independent source/executable/physical checkpoint audit passed; the independent full-value comparator rerun also passed. The selected2025 native Parquet exports and checkpoint metadata were inspected and hashed. Logged checkpoint data size472898864bytes matches the saved data inventory; it is distinct from the2474766670-byte recovered output.

The original1800-second watchdog attempt failed before completion and remains preserved. The unchanged workload passed with only the caller timeout extended to7200seconds. SQL child duration5741.06seconds; complete driver duration5937.16seconds. Inputs, payloads, batches, memory limits and backend pools were unchanged. No performance target or SLA was declared.

The retained-state claim uses the append-only COUNT/MAX contract and exact all-key checkpoint/final value oracles: at least one full payload is retained per key. Opaque compressed Parquet inventory is not a decoded private-state proof. This is one finite aggregate/RocksDB/controller configuration, not a combined-workload qualification. Workers are recreated inside the same SQL-test process; this is not operating-system process-loss recovery.

## Remaining acceptance

Full STR-32/Arcstream qualification remains open: combined retained state, broader faults/backfill, complete application lifecycle/current integrated source and the live Kafka encoded-journal failure are not established by this increment. PR35's ordered-window repair remains separate and unmerged. The standalone actual24-hour run remains waived in favour of application integration.

The CPU-heavy Arcstream demo was stopped at the user's request; its container and state remain available. [Kafka architecture review](str-32-kafka-recovery-architecture-review.md) records its supported recovery scope and gaps. [Throughput diagnosis](str-32-throughput-diagnosis.md) distinguishes ingestion compaction throttling from output-drain snapshot overhead; no tuning or redesign was applied.
