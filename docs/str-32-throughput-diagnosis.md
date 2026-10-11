# STR-32 capacity throughput diagnosis — 2026-10-11

Current test: 100,000 keys, 8192-byte payloads, source batches32, RocksDB/controller, source e4aec059. Backend pools62MiB include a deliberately small4MiB shared memtable allowance; whole-child RSS ceiling512MiB. This is a correctness/capacity configuration, not a measured production throughput target.

Read-only observation of PID25958 found a RocksDB compaction thread consuming approximately one CPU core. The RocksDB LOG explicitly reports `Stalling writes because we have 20 level-0 files rate 16777216`. Frequent WriteBufferManager flushes produce roughly3.2MiB tables. The evidence establishes compaction pressure and RocksDB write throttling. It does not establish that compaction is the sole bottleneck in every phase.

A three-second host NVMe sample showed less than1% busy and almost no physical reads; it does not show physical disk saturation. Process I/O counters include buffered and cancelled writes and must not be treated as durable disk throughput or a write-amplification ratio. One measured compaction consumed approximately7seconds wall and CPU time, producing approximately446MB.

Possible later tuning: compare larger bounded memtable allocations and additional compaction parallelism against events/s, total memory, checkpoint/recovery duration and write-stall time. No tuning, new backpressure feature or engine change was applied. The user agreed this is a later performance concern; the current correctness workload remains unchanged except for its caller timeout.

Evidence is preserved outside regenerable build caches in `/home/jason/qa-evidence/str32-arroyo-queues-20261011/capacity-live-rocksdb-0.log`; the saved snapshot covers the recovery database; the original live log could rotate during the run. Initial phase stall observations were independently read by checkpoint_reader before database cleanup.

## Output-drain phase

After input completed, compaction activity was modest (approximately13%of one CPU in a five-second sample) and pending compaction bytes were zero in the sampled LOG. Earlier ingestion write throttling must not be assumed to explain this phase.

The dirty-output loop starts a stable transaction scope for each small bounded output chunk. The RocksDB adapter implements that internal read snapshot through a filesystem checkpoint, force-flushing the memtable and opening a separate read-only database. The live LOG shows `Get Live Files` flushes about every0.4–0.6seconds, with approximately10changed groups per flush. This is internal snapshot work, distinct from the saved pipeline checkpoint/recovery protocol.

Independent source/LOG review approved this finding. Snapshot churn is a strong bottleneck candidate, but exact time fractions for snapshot creation, reads, evaluation and serialization have not been measured. The local SQL harness has no established external metrics endpoint. This is a separate implementation finding from standard ingestion tuning; no repair or architectural replacement was implemented or implicitly approved.

Source-bound evidence: `/home/jason/qa-evidence/str32-arroyo-queues-20261011/throughput-drain-evidence.json`, `.md` and `throughput-drain-independent-review.json`.

## Build profile

The required development image uses the cached RocksDB10.4.2 archive built atO0, documented in `.claude/build-test.md`; the pinned SQL-testing executable is a development/test build. These rates are not production throughput qualification. A separate optimised-build benchmark with realistic bounded backend budgets would be required before drawing a production capacity conclusion; no rebuild was launched for this correctness run.
