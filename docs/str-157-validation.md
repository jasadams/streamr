# STR-157 — inline MERGE relational source materialization

Base: `476baa5b096e27158cec4f956d6bbe93dbd6bd3b`.
Worktree: `/home/jason/repos/streamr-worktrees/str-157-inline-merge-20261026`.
Instructions read: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, full Trakkt STR-157,
and feature-implementation-engineer role. CLAUDE/CODING_STANDARDS absent.

## Established contract and repair

`plan_named_merge` resolves/binds an ordinary source SELECT and derives key,
clause and capture expressions from its projected schema. The graph visitor
(`builder.rs`, `TreeNodeVisitor`) visits only nontransparent streaming extensions;
ordinary Projection/Filter nodes require a physical value-calculation boundary.
The upstream-derived pre-state-table builder (`ad90cee9^`) has the same visitor
contract. CREATE VIEW already supplies `RemoteTableExtension` (`tables.rs`).
Inline MERGE omitted it, sending raw rows directly to projected expressions.

The repair wraps the resolved source in that existing nonmaterialized extension.
Its physical plan evaluates the original native SELECT before state access.
Names, schemas, predicates and bound event clock are preserved. There is no
new public syntax, backend branch, ownership route, checkpoint protocol or
resource admission. The worker constructor schema guard is unchanged.
No RocksDB scans are added or changed.

## Native regression

`inline_merge_materializes_filtered_and_reordered_source` plans dropped,
reordered and same-width source projections and checks the actual fused graph's
raw input, preceding native value step and state-access expression schema.

`inline_merge_filter_projection_checkpoint_fresh_worker_memory_rocks` runs the
SQL in `local_subquery_recovery_tests.rs` through the planner and decoded fused
worker. It supplies all four raw source columns and excluded A/B rows directly
to `process_batch`; tests do not prefilter/project rows. Twelve combinations:
memory/RocksDB × `k,payload` / `payload,k` / `owner,k,payload` × inline/named view.
Literal typed captures assert source column positions, old/new keys and payloads,
insert/update action, and native nanosecond timestamp17. Prefix C key1 inserts
`first`; excluded A key1/B key2 produce no captures. Continuation excludes B
key1/A key3, updates C key1 to `replacement` with old `first`, and inserts C key2
`second` with no old row, proving excluded B did not populate it.

Final known-key point reads assert key1=`replacement`, key2=`second`, key3 absent
in both uninterrupted and restored workers, without enumeration.

The test exports a nonempty typed checkpoint, closes the original worker/backend,
constructs a fresh decoded worker/table manager, restores serialized protobuf
metadata, and compares complete continuation captures with uninterrupted output.
This is operator/backend recovery, not controller selection, Kafka replay,
distributed barriers or process-kill qualification.

## Validation

Purpose: focused correctness/recovery, normal dev/test profiles, matching
precompiled RocksDB, maintained shared queue and shared target. Disk before build:
102GiB available. `STREAMR_DEV_IMAGE=localhost/streamr-str127-dev` selects existing
compatible image `bad108ab1443`; actual host/runtime native selector remains on.
The default stale `arroyo-dev` image rejected the initial fmt command before any
Cargo work (exit2, missing matching prebuilt archive); no image was rebuilt.

- Maintained `scripts/cargo-dev fmt --all` with image override: exit0.
All commands below use the image override above. Logs and source snapshots:
`/home/jason/qa-evidence/str157-inline-merge-20261026/`.

| Command | Exit / result | Log |
| --- | --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-planner -p arroyo-worker` | 0 | `check.log` |
| `scripts/preflight-clippy.sh -p arroyo-planner -p arroyo-worker` | 0; all features/targets, strict warnings | `clippy.log` |
| `scripts/cargo-dev test --locked -p arroyo-planner --lib test::continuous_merge --no-fail-fast -- --test-threads=1` | 0; 64 passed, 0 failed/ignored | `planner-tests.log` |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib arrow::state_table --no-fail-fast -- --test-threads=1` (initial) | 101; 30 passed, 1 failed, 1 ignored | `worker-tests.log` |
| `scripts/preflight-clippy.sh -p arroyo-worker` (after helper repairs) | 0 | `clippy-repair1.log` |
| `scripts/cargo-dev fmt --all -- --check` (final) | 0 | `fmt-final.log` |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib inline_merge_filter_projection_checkpoint_fresh_worker_memory_rocks --no-fail-fast -- --test-threads=1` (final) | 0; 1 test passes all 12 matrix arms | `worker-scenario-final2.log` |
| `scripts/cargo-dev test --locked -p arroyo-worker --lib arrow::state_table --no-fail-fast -- --test-threads=1` (final) | 0; 31 passed, 0 failed, 1 ignored | `worker-tests-final.log` |
| `scripts/cargo-dev test --locked -p arroyo-planner --lib inline_merge_materializes_filtered_and_reordered_source --no-fail-fast -- --test-threads=1` (boundary temporarily removed) | 101; expected schema assertion failure | `planner-negative.log` |
| Same planner command after exact source restore | 0; 1 passed | `planner-restored.log` |

The sole existing worker ignore is
`like_regex_decoded_complete_sql_and_source_batches_1_8`, requiring an external
current-source LIKE_REGEX fixture export. No ignored test is reported as passed.
Final related tests include existing scalar checkpoint/fresh-worker batches1/8,
LIKE_REGEX native guard cases and table/owner/resource/atomicity regressions.

Initial selected inventory completed on one revision before repairs. Its only
failure was the new helper counting zero-row captures for excluded event
envelopes: first memory/inline/dropped arm stopped at prefix count3 vs1.
The test-side extraction now discards zero-row captures; raw rows still enter
the worker unchanged. Point readbacks were added in the same batch. The next
exact scenario attempt (`worker-scenario-final.log`, exit101) passed first-arm
prefix, uninterrupted continuation and point reads, then stopped at restore
because its shorthand export path violated the existing typed checkpoint file
ownership convention. This fixture now uses the same epoch/operator/table path
as the existing recovery test. Neither failed attempt completed the12-arm matrix.
No production runtime or checkpoint code changed to address these fixture errors.

The second worker Clippy pass covers the complete helper/point-read changes;
its exact source and hash are `worker-tests-clippy-repair1.rs` and
`clippy-repair1-source.sha256`. The sole later code edit is that checkpoint path
format expression. It is covered by final scenario and full worker tests;
Clippy was not repeated for this fixture-only path correction. Locked check
covers unchanged production code; planner suite covers unchanged planner files.

Full final worker suite and planner negative/restored controls ran under one
outer `scripts/rust-build` reservation (`remaining-validation.sh`, exact exits
in `remaining-exits.txt`). Admission performed scheduled cache eviction before
that reservation; no extra eviction occurred between its commands. All commands
used the shared target and matching dev/test prebuilt native selector.

The negative control removes only the13-line production boundary. The regression
then fails with actual input `[owner,k,payload,_timestamp]` instead of expected
`[k,payload,_timestamp]`. Exact fixed source restoration verified SHA256 before
the restored test passed (`production-restore-verification.txt`). This establishes
that the regression detects the original lowering defect.

Final source SHA256 (`final-source-sha256.json`, complete copies in `source/`):

- `continuous_merge.rs`: `d6d095c8230b7d8eca6f6e0e01332b90dc90003b25235e42a6aec6b2fd633820`
- planner `test/continuous_merge.rs`: `58d981ff3cd7f0fdb3c454f89aa61f3a098703922dbc32b2d31c564058c3b5fd`
- worker `local_subquery_recovery_tests.rs`: `d96009b83a5348cb87732525cf5285191111b4cd0e5c9c29d349547fa6e0c3e8`

Independent final source/evidence review APPROVED with no findings or deferrals.
Preserved source hashes, all selected results and negative/restored controls were
independently audited. Publication and current-head CI remain delivery gates.
No full CLI, controller/Kafka/process-loss or broader qualification is claimed.
