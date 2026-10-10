# STR-102 validation

Base: `130f43a265551883e8a13c1d55d51697e27e7f9f`, fetched main.
Instructions: `AGENTS.md`, `.claude/team.md`, `.claude/build-test.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, current STR-102.

Evidence root: `/home/jason/qa-evidence/str102-20261010/`.
Container image ID:
`a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`.
Shared target: `/home/jason/repos/streamr/target/milestone2-runtime`.
Builds and runtime captures held serialized queue reservations. Generated RPC
artifacts were refreshed by touching the unchanged proto inside each reservation.

## Current checks

`str102-loader-gates.log` records exit 0 for:

- `cargo-dev check --locked` for worker, planner, RPC and SQL testing.
- Worker `arrow::updating_join`: seven physical-plan regressions, including
  bounds, cancellation, compaction, logical NULLs, bag identities and LEFT
  transitions; populated/empty checkpoint restore on both backends.
- Worker `engine::native_join_admission_tests`: one regression for serialized
  updating-join admission, invalid limits, unsupported joins and malformed plans.
- Planner library: 219 passed, one existing opt-in ignored.
- Strict `preflight-clippy.sh` for the four affected crates; formatting check.

The Python oracle self-test has 16 passing adversarial tests; `py_compile` and
staged whitespace checks passed.

## Actual SQL and fresh workers

`sql-matrix-attempt4-summary.json` audits all 32 receipts as passed/exit 0.
The variants are direct INNER, direct LEFT, chained INNER and downstream updating
aggregate; each ran memory/RocksDB × controller/leader × source batches 1/8.
The independent oracle checks complete output values, every Debezium before/after
transition, stable bag identities, checkpoint output and fresh-worker continuation.
`docs/str-102-fixtures.md` describes the fixture and oracle contracts.

The executable was built and copied outside regenerable caches in the same queue
reservation as attempt 3. Attempt 4 reused that pinned executable with explicit
existing scan/write pool overrides; its receipts bind each SQL/input/configuration
and the final driver hash to the independent build receipt.

- Build receipt: `sql-matrix-attempt3/source-build-receipt.json` (build exit 0).
- Executable SHA-256:
  `c47b2866efdc52960b77b14d62de88a21fb5d51050a6e9aa77a025e5dd159b84`.
- Built source patch SHA-256:
  `db38702c30ae4677209d1935f9f7f2bc83c2ff3f9f5bc78f2824a4b0ecb3ae26`.
- Passing captures: `sql-matrix-attempt4/`; queue log:
  `sql-matrix-attempt4.log`.

After executable pinning, engine changes only reflowed the admission test through
rustfmt. Harness changes set the existing queued-write override to 64 MiB and
assert/document it; ledger/validation prose records final outcomes. The preserved
`post-pin-source-delta.patch` allows independent comparison with the built source.
No production behavior changed after pinning.

## Preserved failures and qualification boundary

Attempt 1 failed checkpoint capture because the inherited 2 MiB scan pool could
not provide a page at the fixture's eight-database ceiling. The driver now selects
16 MiB through the existing override. Attempt 2 exposed omitted serialized
RocksDB join admission/owner counting; the reviewed engine repair and new
regression address it. Attempt 3 started RocksDB execution but exhausted the
inherited 32 MiB queued-write pool: four actual owners each reserve slightly over
10 MiB. The driver now explicitly selects 64 MiB. All failures and partial passes
remain preserved; product admission limits and checkpoint formats are unchanged.

Larger-than-RAM capacity, measured RSS, broader slow-consumer/fault and soak
qualification remain in the existing STR-32 shared batch, as required by the
milestone feature plan and backlog-fast workflow. This increment establishes
functional regressions and reviewed bounds, not full milestone qualification.
