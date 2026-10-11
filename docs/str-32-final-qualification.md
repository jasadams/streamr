# STR-32 delivered engine increment

The user split remaining capacity qualification into STR-103 and production
process recovery into STR-104 on 2026-10-11, then authorized merging PR36 and
PR37. Both follow-ups are High priority, agent-ready, milestone 3. Broader engine
fault/operator qualification remains STR-27; application readiness remains
ARC-17. This increment does not claim full production qualification or deploy
an application.

PR36 restores Arroyo execution and row backpressure, removing the added
execution-memory admission and unused helper APIs. PR37 uses lifetime-safe native
RocksDB read snapshots, releases each bounded read before downstream awaits,
and preserves supported ordered native window aggregation without restoring
execution admission. Saved checkpoint/recovery contracts remain unchanged.

## Exact validation

Compiled candidate: `75e430b4e2473c15a5fa752c5196be7b5e249ef1`.
Required Bookworm image:
`a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`.
SQL executable SHA256:
`05482f6d2e81d250c1d2b842f4130c0233059ca09309daeb43ee847740e68f05`.
Production executable SHA256:
`58304f3b65bf10bacc42fffb56e09eec6304d3b50a75efe8841749c0d89d540b`.
All evidence is preserved under
`/home/jason/qa-evidence/str32-final-qualification-20261011`.

| Check | Result and precise scope |
|---|---|
| Container fmt, locked state/worker check, strict Clippy | PASS |
| State/worker libraries | 71 + 165 = 236 PASS |
| Aggregates/updating joins/default state tables | 8/32/8 configurations independently PASS, full values and recovery prefixes |
| State-table lifecycle | 8 configurations, native/switched backends through three epochs, independently PASS |
| Calendar filters/pruning | 16/8 independently PASS, typed CDC, timed quiet-key observations, physical prefixes and fresh suffixes |
| Unordered aggregate values | 8 independently PASS |
| Ordered aggregate fixture | Existing native memory/controller batches 1/8 and two baseline cases independently PASS; additional six configurations are recorded separately when completed |
| Native SESSION | Normal 8 plus late-input 4 independently PASS; late driver intentionally uses batch 1; exact plain JSON values and empty-prefix continuation |
| Top-five/session reuse/continuous/hot cases | 26 independently PASS; maximum reported SESSION RSS 209793024 bytes, below 512 MiB; no >=10x retained-state claim |
| Export refusal/retention | 12 independently PASS; injected NotADirectory refusal, not actual ENOSPC or process loss |
| External application full SESSION fixture | 8 runner PASS; complete application readiness is outside STR-32 |
| Production process loss | Active run and existing results handed to STR-104; not claimed as full qualification |
| Combined >=10x retained-state capacity | Pending STR-103 |

The earlier optimized matched aggregate benchmark took Streamr 14.34 seconds
versus Flink 11.95 seconds (1.20x runtime), approximately 5x faster than the
prior unoptimized-native build. That receipt has its own source provenance and
is not relabeled as the final ordered-integration executable or general SLA.

## Reviewed test-fixture repairs

The lifecycle fixture now projects its declared 18/34/34 payload fields explicitly
rather than expanding a view's internal timestamp via SELECT *. Exact corrected
query bytes passed the existing full-row oracle across all eight configurations.
The production checkpoint decoder expects existing watermark state version 2,
with Impulse and file-sink global versions still 0. Its one-line correction was
independently reviewed and decoded saved epochs 1/2, source counters, sink offsets
and complete committed values offline. Neither correction changes engine semantics.

Original failed attempts, missing-log attempts and their successful distinct
replacements remain preserved. Deployment, release, live cutover and follow-up
qualification are separate from these merges.
