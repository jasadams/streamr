# STR-107 event-clock validation

Base: `1766a63d663210ee15b2925ae194d4be60a985f1` (fetched origin/main).
Instructions: `AGENTS.md`, `.claude/build-test.md`,
`docs/build-qa-troubleshooting.md`, `docs/milestone-3-status.md`,
`docs/milestone-3-work-plan.md`. `CLAUDE.md` and
`docs/CODING_STANDARDS.md` are absent at this base.
The full [ticket](https://trakkt.app/issues/STR-107) owns all eight acceptance criteria.

## Contract and existing ownership

`EVENT_TIMESTAMP()` resolves to the current triggering row's internal
`_timestamp`; `EVENT_DATE()` casts that timestamp to UTC Date32. Both markers
retain existing event-clock provenance and are eliminated before worker physical
execution. Lack of explicit `WATERMARK FOR` no longer rejects a single event
source. Zero sources, multiple triggering sources, overwritten internal time and
aggregate-output time retain explicit errors. Lookup event ordinals remain the
clock authority; retained target timestamps do not become drivers.

`SourceRewriter::projection_expressions` already uses the designated FOR payload
when present and otherwise retains the source `_timestamp`. The same route is
present in upstream-era commit `74c967e343bfa0d0741578c01aeb356edb613980`.
The unchanged Impulse operator at that commit and this base emits
`state.start_time + interval * state.counter` in interval mode and
`SystemTime::now()` otherwise. Its existing global table `i` checkpoints both
counter and start_time. No new source fields, clock or source-state format is
introduced; wall-clock mode is not claimed intrinsically deterministic on replay.

Calendar contribution dates remain separate from the triggering reference date.
The existing descriptor/runtime route, CDC envelope clock, watermark AS progress,
late admission, quiet-key maintenance, pruning and checkpoint protocol remain
unchanged. Supported finite local expressions consume correlated immutable
EVENT values exposed by a driving projection/view; local rowsets own no source
clock. Ordinary two-stream joins remain ambiguous even when they reuse a table.

## Current-source evidence

Receipt root outside Cargo caches:
`/home/jason/repos/streamr-validation/str-107-event-clock-20261012`.
Development image: `e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Initial free space: 210 GiB (78% used). One shared container target and serialized
queue are used. Historical STR-61/62 receipts and hashes remain unmodified.

SQL-testing executable SHA256:
`661f5b94605918fc2f04edbf918e5066d7f91a422de3c4bbf0370cb3a67f5764`.
It is pinned outside Cargo caches and verified before each repaired matrix.
`inventory.json`, `repair1-inventory.json` and `repair2-inventory.json` retain exact
commands, exits and durations; their named logs retain individual assertions.
`source-files.json`/`source.diff`/`source-receipt.json` describe the first execution
source (patch SHA256 `0aea79c5e804383be5315a4ecc82cd59a2457191e2cc6a4352b7d3668f179976`).
Repair receipts preserve later fixture changes separately. Production and compiled
SQL-testing helper hashes remain identical; the repaired fixtures use that same
executable. Initial planner executable was not separately pinned; repair1 and
repair2 planner executables have separately attributed binary receipts.

## Commands and collected outcomes

Container commands use the repository serialized build queue, development image,
shared `target/milestone2-runtime` and locked dependencies. Every process exit was
consumed. Focused Cargo tests use `--locked --no-fail-fast -- --test-threads=1`.

| Command / scope | Final result | Preserved earlier failures |
| --- | --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-planner -p arroyo-worker -p arroyo-sql-testing` | PASS exit0 (`check.log`, `check-repair1.log`) | None |
| `scripts/preflight-clippy.sh -p arroyo-planner -p arroyo-worker -p arroyo-sql-testing` | PASS exit0 (`clippy-retry1.log`, `clippy-repair1.log`); final planner-only lint PASS exit0 (`clippy-final.log`) | Initial exit1 missing trait import in test capture helper; repaired |
| `cargo test -p arroyo-planner --lib test::event_clock` | PASS18/18 exit0 (`planner-event-clock-repair2.log`) | Initial16/17: hidden source column in fixture; repair1 17/18: wildcard view sink arity |
| `cargo test -p arroyo-planner --lib test::local_subqueries` | PASS8/8 exit0 | None |
| `cargo test -p arroyo-worker --lib calendar_` | PASS12/12 exit0 | None |
| `cargo test -p arroyo-worker --lib signed_timestamp_tests` | PASS6/6 exit0 | None |
| `cargo test --locked --no-fail-fast -p arroyo-sql-testing --no-run --message-format=json` | PASS exit0; executable pinned within same queue reservation | None |
| `python3 scripts/test-native-event-clock.py /evidence/arroyo-sql-testing --directory /evidence/event-clock-repair2` | PASS56/56 exit0 | Initial8PASS/48FAIL; repair1 48PASS/8FAIL |
| `python3 scripts/test-native-calendar-filters.py /evidence/arroyo-sql-testing --directory /evidence/calendar` | PASS16/16 exit0 | None |
| `python3 scripts/test-native-calendar-pruning.py /evidence/arroyo-sql-testing --directory /evidence/admission-pruning` | PASS8/8 exit0 | None |
| `python3 scripts/test-native-impulse-event-clock.py /evidence/arroyo-sql-testing --output-dir /evidence/impulse` | PASS4/4 exit0 | None |

The first full inventory collected all selected failures before batch repairs.
Forty event-clock failures came from historical fixtures whose pre-epoch,
out-of-order and CDC rows were now below established common admission, plus an
obsolete per-image CDC clock expectation. The repair keeps event payload fields,
durations and exact signed images, sets the independent AS column below all
intended admitted rows, and independently asserts delayed progress. Dedicated
pruning tests still assert equality/late rejection. Eight local-expression failures
came from the separate direct-INSERT schema limitation below. The supported view
route initially used `SELECT *`, which expanded retained context columns; explicit
public sink columns repaired that fixture. No production admission or clock
behavior was changed to satisfy these tests. Historical failed receipts remain.

Final static inventory (`static-final-inventory.json`): formatting, all three
independent oracle self-tests and both whitespace audits exit0. Python syntax
compilation passes. `obsolete-audit-final.log` contains only the intentional
old-name rejection test. Final source receipts include tracked-file SHA256 and
staged patch SHA256; `repair2-planner-binary.json` pins the final planner test
executable (`4c23b41ca251b2212789ee86630662f85823c3a10768a9c18f65c3dae153074f`).

## Acceptance mapping

1. **PASS:** planner18 and actual event-clock56 cover both functions on explicit
   FOR sources, serialization/physical reconstruction and decoded execution;
   Impulse4 execute existing implicit timestamps without new DDL/fields/override.
2. **PASS:** event-clock exact external fixtures assert nanosecond precision,
   pre-epoch UTC dates, midnight and independent FOR/AS columns with delay.
3. **PASS:** admitted backwards references retain exact values in event-clock;
   admission/pruning8 and worker signed-timestamp6 retain late/equality controls.
4. **PASS:** omitted/aliased timestamp views, conflicting retained lookup clocks,
   correlated finite local values and native calendar COUNT/SUM execute;
   planner18 assert zero-context, ambiguous, overwritten and aggregate diagnostics.
5. **PASS:** CDC start/end cases assert coherent current-change clock for signed
   before/after images and exact independent payload durations/values.
6. **PASS:** all84 actual scenarios use memory/RocksDB and batches1/8, existing
   checkpoint machinery and fresh-worker restores with exact committed prefixes
   and continuation. Impulse4 read saved table `i` counter/start_time and independently
   derive nanosecond timestamps; source observations verify actual batches1/8.
   Wall-clock fallback is not claimed deterministic replay.
7. **PASS:** obsolete call rejection tests and maintained-name audit; historical
   evidence stays attributed to its original source and hashes.
8. **PARTIAL pending coordinator delivery:** affected check/strict lint and focused
   collected inventory pass; independent final-diff/evidence review approved all21 files with no remaining findings; current-head
   PR CI is still required. No acceptance1–7 is deferred to manual QA.

## Separate limitation and publication

[STR-108](https://trakkt.app/issues/STR-108) tracks direct INSERT nested VALUES
schema propagation. The preserved direct attempt is
`event-clock/memory-controller-batch1-append-local/query.sql` with `capture.log`.
Pinned DataFusion `916b45f5`, `datafusion/sql/src/values.rs` reads the outer INSERT
sink schema through `planner_context.table_schema()`; `values_with_schema` then
requires five outer columns for a one-column local VALUES. Supported finite local
expressions are validated through an evaluated view and explicit public sink
projection in all eight backend/batch/recovery configurations. This separate
limitation does not defer STR-107 acceptance or authorize a dependency redesign.

Independent final-diff/evidence review approved the full21-file diff and raw acceptance evidence with no remaining findings. Written review is preserved in the canonical review log for2026-10-12. [PR #41](https://github.com/jasadams/streamr/pull/41) is published; current-head CI and merge handoff belong to the coordinator. STR-103 capacity qualification remains separate and NOT RUN.
