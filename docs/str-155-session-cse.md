# Native SESSION aggregate CAST schema consistency

STR-155 starts from fetched `origin/main`
`ac15b4e9dec7bfa5a8ea080a712c71897c520ca5`. Instructions read:
`AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md` and `docs/milestone-3-work-plan.md`.
The precise cause and proposed repair were reported before implementation.

## Failure and cause

The captured generic SQL is preserved verbatim in
`crates/arroyo-planner/src/test/queries/str155-session-cast.sql`, SHA-256
`5c59d35745fc1aed1391177f6841cfedc01eece0bcda6418f8508ad3cc00a214`.
Original SQL/error evidence lives under
`/home/jason/qa-evidence/str154-native-kafka-checkpoint-stall-20261023/old-64-partitioned/`.
It rejects during planning with `SESSION final reader differs from registered input`.
This rejection does not demonstrate a runtime checkpoint stall or establish
the cause of STR-154.

DataFusion common-subexpression elimination legitimately projects
`CAST(ts AS BIGINT)` once as `__common_expr_1`. The subsequent Streamr
`UnnestRewriter` rebuilds projection schemas from rewritten children, including
projections without UNNEST. Generated `ts` and the shared cast become
nonnullable. The surrounding `KeyCalculationExtension` replaces its child but
retains its cached nullable logical schema. Its graph output uses the current
child schema, whereas its physical reader uses the cached schema. The native
SESSION proof correctly detects that difference.

The cached-schema behavior is inherited from
[upstream Arroyo's key-calculation extension](https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-planner/src/extension/key_calculation.rs).
Upstream also plans SESSION's aggregate input directly in
[its aggregate extension](https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-planner/src/extension/aggregate.rs).
The projection re-inference was introduced in Streamr commit
`a2c823f1e216c1a5bd888e57d1818cbef50902e5` for typed/correlated JSON_TABLE.
It is needed for parent projection schemas after child rewriting. The repair
therefore updates the stale keyed-reader schema rather than suppressing that pass.

## Repair and regression coverage

Ordinary unnamed `Keys` readers now refresh their logical schema from their
replacement child. Named/trimmed and expression-key layouts retain their
existing handling. Selector ordinal binding still appends its persisted ordinal
only to the physical aggregate reader. CSE, SQL values, raw event-time primary
ordering, watermark/window clocks and checkpoint protocols are unchanged.

The focused regressions check rewritten field nullability and preservation of
trimmed/expression-key layouts, then plan the complete captured SQL through its
native SESSION and downstream MERGE. They inspect retained CSE output, actual key
projection/registered-input equality, the final reader plus ordinal and both
selectors' raw-clock primary ordering.

## Validation status

Validation purpose: focused correctness, normal dev/test profiles, matching
precompiled RocksDB archives, canonical shared target and serialized queue.
The stale default `arroyo-dev` tag rejected the first formatting invocation
before running Cargo (exit 2). Supported `STREAMR_DEV_IMAGE` selects immutable
image `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`;
the maintained script accepts its native pin and selects prebuilt mode.

Independent source review approved the repair with zero blocking findings.
Current-source formatting, locked planner check and maintained strict Clippy
(all features/targets) passed. Exact gate commands, exits, durations and source
hashes are preserved under
`/home/jason/qa-evidence/str155-session-cse-20261025/planner-gates/`.
The final selected tests passed with `--locked --no-fail-fast`:

- Keyed-reader layout regression: 1 passed, 0 failed.
- Continuous-MERGE module, including the complete captured SQL and its actual
  predecessor/edge/CSE/reader/selector proof: 63 passed, 0 failed.
- Native-capability module: 14 passed, 0 failed.

All seven final commands exited 0. Source review also approved the repaired
test diff. The earlier attempt remains under `planner-gates-attempt1/`:
the synthetic unit input omitted the required `_timestamp`, and the captured-SQL
test incorrectly counted every routing adapter carrying the CSE column.
The repaired tests supply the canonical timestamp and bind the shared CAST
proof to SESSION's actual graph predecessor and edge rather than a graph-wide
adapter count. Production code did not change during these test repairs.

## Native Kafka runtime results

The maintained normal-dev/prebuilt CLI pin is SHA-256
`b1a00509f9ab4cf76acefbf61cd5620a8c714bf35c1850c1c0a0cd687ba9aaf4`.
Pin, native archive and source provenance are retained in `dev-pin/`, including
`source-manifest.json` and `image-native-receipt.json`, under
`/home/jason/qa-evidence/str155-session-cse-20261025/`.
Independent review verified all1344 compiled inputs match this pin.
The four-case inventory completed on this same pin with no engine, SQL value,
clock, batch-size or oracle changes. Connector address/namespace substitutions
are recorded separately in each case's `query-binding.json`.

| Backend | Scenario | Result | Observed committed output | Restored checkpoint |
| --- | --- | --- | --- | --- |
| Memory | Unpaced uninterrupted | FAIL, exact130 timeout | 66 inserts, 0 closure updates, 0 closed sessions | No worker-loss case |
| Memory | Actual worker SIGKILL after the 64-input prefix | PASS | Exact 130 typed transitions, 64 closed sessions; physical prefix unchanged | CP1, partition0 next offset64 |
| RocksDB | Unpaced uninterrupted | FAIL, exact130 timeout | 66 inserts, 0 closure updates, 0 closed sessions | No worker-loss case |
| RocksDB | Actual worker SIGKILL after the 64-input prefix | PASS | Exact 130 typed transitions, 64 closed sessions; physical prefix unchanged | CP1, partition0 next offset64 |

The full outer runtime inventory exited **1**, preserving both failures.
`runtime-exits.json` records case exits `1,0,1,0`; each
`runtime-{memory,rocksdb}-{liveness,recovery}/result.json` preserves its verdict.
Recovery cases retain `fault.json`, `actual-restored-checkpoint.json`, selected
and actually restored checkpoint artifacts, physical prefix snapshots and final
typed outputs. The later final completed checkpoint is distinct from CP1, which
was actually restored after the worker loss.
Owned-resource cleanup completed for all four cases.

## Unpaced acceptance gap and native watermark evidence

The unpaced 130-transition oracle remains **unmet**. It is not counted as a pass
or replaced by the recovery result. Read-only diagnosis of the memory liveness
case decoded all retained complete checkpoints, epochs46–60. Each records source
partition0 next offset66 and these actual native watermark values:

| Saved field | UTC value |
| --- | --- |
| `last_watermark_emitted_at` (maximum raw timestamp at emission) | 2020-01-01T00:32:00Z |
| `max_watermark` | 2019-12-31T23:59:55Z |
| `emitted_watermark` | 2019-12-31T23:59:55Z |

The old64 sessions have 00:30 deadlines, so the saved watermark cannot close
them. The native generator takes the minimum AS expression over an admitted
source batch, retaining progress across batches. Its response root can split
rows after that calculation, but does not recalculate per-row watermarks.
Checkpoints save state; ticks/Idle and Kafka catch-up do not advance finite event
time. The actual admitted batch lengths were **not measured**: default512 and
unpaced input do not establish that exactly one66-row batch occurred.

This batch-min behavior is present in
[upstream Arroyo at immutable commit c8a659c540974e60e0d1311070052846697ea5f4](https://github.com/ArroyoSystems/arroyo/blob/c8a659c540974e60e0d1311070052846697ea5f4/crates/arroyo-worker/src/arrow/watermark_generator.rs#L165).
`watermark-upstream/receipt.json` retains fetched URLs, source bytes and hashes.
`watermark-contract.json`, `watermark-contract.md` and
`decode-watermark-contract.py` retain exact checkpoint references/hashes, raw
bincode payloads, decoded values and the reproducible read-only diagnosis.
Inspected runtime source files match the actual pin's frozen inputs byte-for-byte.

The separately admitted suffix after the recovered64 prefix permits native
watermark progress and produced the passing130-transition recovery results.
STR-155 changes none of the source, watermark or SESSION deadline paths above;
the observed insufficient watermark is existing native behavior, not a
STR-155 regression. No code work was cut or deferred from the schema repair.
The unpaced case remains a separate acceptance gap; native batch-min semantics
were preserved rather than changed to satisfy it.

Independent final review approves the narrow schema repair and exact
both-backend recovery evidence. It also independently decoded insufficient
SESSION watermarks on both backends; the detailed generator-state decoding
above covers memory only. The complete four-case acceptance remains unmet and
CI is pending. No all-green runtime, full campaign, performance, production
deployment or STR-154 liveness result is claimed.
