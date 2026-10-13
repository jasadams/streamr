# STR-109 throughput repair

The user requested throughput improvement before renewed STR-103 correctness
qualification. Work starts from fetched main
`c9c8363a3c772f3648a463278cf53ee348028734`. Instructions: `AGENTS.md`,
`.claude/build-test.md`, `.claude/team.md`, and the milestone 3 ledger/plan.

## Evidence and limits

The stopped four-owner run committed 8,400 events between checkpoints 82 and 87
in 1,237.190653 seconds: 6.789576 events/sec, with 546,800 events remaining.
The source targets 1,000 events/sec. Checkpoint export was in flight for 51.47%
of this interval; that does not prove processing paused throughout export.

The earlier Flink comparison measured Streamr at 2,091.51 events/sec for a
single aggregate with 10,000 keys and 30,000 events. Its graph, retained state,
source revision and measurement boundaries differ from STR-103; this is not a
same-workload regression comparison.

A bounded diagnostic restored the unchanged checkpoint 87 into generation 10
in 518.835 seconds. Independent review verified all 14 tasks, original source
clock/counter, worker identity and committed output prefixes. The first CPU
capture wrote 2,107 samples over approximately 30 seconds. The collector
incorrectly rejected its expected controlled SIGINT exit before recording
after-metrics and final phase checks. The diagnostic failed and cleaned up all
owned processes/container. These samples are partial CPU evidence, not a
complete processing/checkpoint comparison or a throughput pass.

`rocksdb::VersionSet::GetTotalSstFilesSize` accounts for 190/2,107 samples
(9.02%), the largest resolved single leaf. Unresolved leaf samples and sparse
callchain coverage limit attribution. The locked RocksDB 10.4.2
[implementation](https://github.com/facebook/rocksdb/blob/v10.4.2/db/version_set.cc#L6952)
walks SST metadata across all retained versions and builds a deduplication set.
Streamr calls this property after every write, while holding the write lock.
This establishes a growth-dependent telemetry cost; it does not establish the
complete cause of the slowdown.

## Authorized change

The user approved sampling native-health telemetry at most once per second
during writes. Initial sampling remains immediate. Cached metrics retain their
availability and sample-age reporting; idle databases can have older samples,
as before. Logical accounting remains per committed batch. Disk reserve checks
still run before every write, with unchanged resource limits, SQL semantics and
checkpoint/recovery behavior. Property measurement stays outside the cache
mutex so metrics scrapes can read cached observations during measurement.

## Validation

Independent source review approved the current change after resolving a cache
mutex finding. Locked state check and strict Clippy passed in immutable
Bookworm image `e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Final formatting and all three focused tests passed, including real
memory/RocksDB logical/native health accounting, deterministic sampling expiry
and cache access during measurement. Implementation commit:
`047535db5bf408cdd0e7f12922ac83150933f6e5`; exact source/log hashes are in
`validation-checks.json` under the evidence directory. Optimized build passed
in 1,821.063 seconds; preserved executable SHA256:
`bf42ebba792599b1c65855c55a7804d4f6c34d49c12b96fd81db0802bc664fd9`.
Independent actual pin review approved all compiled source/package/native archive
proofs. The first paired baseline restored successfully, but manual readiness
review missed its fixed 75-second capture window. It exited 2 before any
profiler capture. The failed run and mutated clone are preserved.

Fresh paired clones each passed 116,094 file hashes and independent audits.
An independently authored, root-reviewed automatic readiness validator removes
live reviewer dispatch from the measurement deadline. Both arms completed
steady and natural-checkpoint CPU/counter captures and exited 0 with owned
process, profiler and container cleanup verified. Both actual runs passed independent review. Initial CPU analyses completed;
callchain coverage remains limited and exact-image symbol analysis is pending.

| Measurement | Baseline c9 | Candidate 047 |
| --- | ---: | ---: |
| Native restoration | 421.957 s | 233.118 s |
| Steady source emissions | 191.461/sec | 315.423/sec |
| Checkpoint-window source emissions | 3.139/sec | 0/sec |
| Sampled peak owned RSS | 431,157,248 bytes | 348,303,360 bytes |

Restoration took 44.8% less time and steady source emissions increased 64.7%.
The counter windows use actual monotonic HTTP intervals after the same 75-second
warmup; each pair restored checkpoint 87/source 653,200 with the same graph,
clock, pools, backend and checkpoint cadence. Fresh restoration rebuilds SST
layout. These are source-emission rates, not durable committed throughput;
the historical 6.79/sec run has different boundaries and cannot be used as the
matched baseline. Checkpoint windows show near-zero emissions in both arms;
0 versus 100 emitted events does not establish a throughput regression.
Native owner input counters also increased from about 190/sec to 312–316/sec,
corroborating processing progress across the graph rather than queue fill alone.

Checkpoint cost remains unresolved. Both windows used about 122.6 CPU seconds
in 31.8 wall seconds. Source tracing confirms normal barriers queue asynchronous
export; the full export itself is not awaited. Shared two-slot native scans,
operator dirty-output draining and synchronous bounded Parquet/compression
work are candidate costs. Initial CPU callchain coverage is poor; no dominant
checkpoint cause or full STR-103 qualification is claimed.

Exact receipts, logs and partial profile data:
`/home/jason/qa-evidence/str109-throughput-20261013/`.
Independent original-preservation audit verified all 116,094 original files
and hashes unchanged after the diagnostic. Failed acquisition remains preserved.
