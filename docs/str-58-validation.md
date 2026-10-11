# STR-58 finite local SQL expressions

Implementation base: `a2c823f1e216c1a5bd888e57d1818cbef50902e5`.
Workspace: `/home/jason/repos/streamr-worktrees/str-58-local-subqueries-20261011`.
Guidance read: `AGENTS.md`, `.claude/build-test.md`, `docs/milestone-3-status.md`,
`docs/milestone-3-work-plan.md`, and the feature implementation agent instructions.
`CLAUDE.md` and `docs/CODING_STANDARDS.md` are absent on this base.

## Implemented contract

`ARRAY(query)`, scalar subqueries and `EXISTS` retain an explicitly validated,
finite native DataFusion rowset. Physical encoding carries the rowset and typed
immutable event bindings; reconstruction checks concrete operators, builtin
implementations, binding ownership and result types. Scans, streaming extensions,
recursive relations, windows and arbitrary/custom or volatile local kernels are
rejected. A scalar rowset has NULL/one-value/cardinality-error semantics; an array
has its native element type, preserves requested ordering and is empty for no rows.

The worker evaluates local queries asynchronously in the existing serial event
owner. Only the selected CASE result branch executes. MERGE assignments read the
same immutable source/prior-row input and finish before pending writes or captures
advance. Native projection/filter wrappers are installed only for plans containing
local queries. Existing asynchronous UDF argument/result composition is explicitly
rejected for this subset rather than accepted and failing at decode/evaluation.

Bound VALUES expressions use native singleton projections and UNION ALL because
upstream ValuesExec evaluates eagerly during physical construction. Local unions
are coalesced to the original single finite relation distribution; native physical
distribution optimization handles aggregates, sorts and joins. No streaming join,
aggregate or sorting history is introduced. No new execution admission pool,
reservation wrapper, public resource knob, backend or checkpoint protocol exists.
Current backend/table limits and checkpoint ownership remain in place.

## Evidence and repaired failures

Logs are retained at `/home/jason/qa-evidence/streamr-str58-20261011/`.

- Core planner local suite passed 10/10 and worker local suite 3/3
  in `local-final.log` (actual exit 0), before final review integrity repairs.
  `local-planner-repaired.log` records an earlier 9/9 exit 0, including exact ARRAY difference/union, scalar MAX/null/cardinality/CASE,
  finite FULL JOIN/COUNT/CTE, NULL set members, typed empty arrays, width rejection,
  nested immediate aggregate/sort correlation, and incremental 1000-member
  first-admitted SQL with blank, unseen earlier and repeated members.
- Worker local suite passed 3/3 in `local-current.log`: native SQL MERGE source/old/
  new isolation, two keys, batches 1/8, memory/RocksDB, typed checkpoint export and
  fresh worker/backend restore with exact continuation against uninterrupted
  execution. The log's overall exit was 101 because then-pending planner cases
  still failed; it is not an overall green receipt.
- Locked affected-package check passed (exit 0), `check.log`. Final-source
  Clippy/formatting/test gates and independent review are recorded below when run.
- Initial failures were the new integration's eagerly evaluated VALUES bindings,
  unnormalized local aggregate distribution, ARRAY_AGG dropping requested order,
  native FROM UNNEST options rejected by the old shared validator, nested aggregate
  output renaming, and multipartition VALUES-derived FULL JOIN. These were repaired
  using native deferred projections, distribution, direct ARRAY rowset collection,
  depth-one UNNEST validation, original output names and finite UNION coalescing.
- The recovery fixture initially declared a 2 MiB scan pool while existing
  unconfigured checkpoint `ensure_empty` requests a 1 MiB page with four-copy plus
  namespace headroom (4,195,496 bytes). Its private test pool now uses 8 MiB,
  matching that established checkpoint contract. Production limits did not change.
- Selected source-bound JSON_TABLE worker checks initially passed 4/5 and failed
  malformed UNNEST options (`json-worker-ignored-final.log`, actual exit 101).
  The shared native-UNNEST relaxation had widened ordinary JSON_TABLE decoding.
  The repaired validator retains strict generated options for ordinary JSON_TABLE
  and separately admits native depth-one options only within validated local plans.
  Fresh independent review approved the split; all five cases now pass.
- Fixture export using Cargo `--config` env triggered the runner's documented
  native-source RocksDB fallback (export passed, 9m49s build). Final repair checks
  use the normal prebuilt mode; fixture tests run its exact pinned binaries in the
  same dev image with runtime environment, avoiding another mode switch.
- The first strict Clippy run rejected one collapsible nested if in new code;
  repaired. Failed receipts remain failed evidence, not passing gates.

## Explicit deferrals

The user decided on 2026-10-11 to retain the current DataFusion dependency and
leave grandparent correlation pending as STR-105.
The pinned Arroyo DataFusion SHA is
`916b45f5c28d94765ae4a6393c5e126b2ea55e1c`. Its frontend replaces the ancestor outer
schema with the immediate input schema. This exact query fails before local
lowering and remains an expected negative regression:

```sql
SELECT (SELECT (SELECT outside.v))
FROM (VALUES (4)) AS outside(v);
```

The diagnostic is a missing `outside.v` field. The upstream limitation is tracked
in [DataFusion issue 15558](https://github.com/apache/datafusion/issues/15558).
No dependency fork or pin was changed. Immediate correlated scopes are implemented;
this deferred acceptance is not claimed passed.

Nested local subqueries inside JOIN ON predicates are rejected during planning,
tracked as STR-106. Native finite FULL JOIN with ordinary equality predicates
passes. This exact deferred shape has a runnable planning rejection regression:

```sql
SELECT (SELECT COUNT(*)
        FROM (VALUES (1)) AS l(v) FULL JOIN (VALUES (1)) AS r(v)
        ON l.v = r.v AND EXISTS (SELECT 1 WHERE l.v = 1));
```

Its diagnostic states `finite local subqueries in JOIN ON predicates are unsupported`.
Native synchronous join predicates cannot await a local rowset; no postjoin filter
rewrite or altered FULL JOIN semantics is introduced.

Persisted ARRAY targets and postfix VARCHAR ARRAY/typed-empty CAST integration
remain STR-63; JSON_OBJECTAGG/map composition remains STR-66. Their implementations
are not duplicated here. Generic JSON_TABLE composition uses merged STR-57.
External application catalog parity, full controller/Kafka replay/barrier recovery,
capacity/fault/soak qualification and production deployment were not run by this
increment. Worker checkpoint continuation is operator evidence, not a distributed
recovery claim.

## Final gates

All commands ran foreground through the shared build queue; actual subprocess and
tool exits were collected. The final repair batch retained one reservation for
compilation, fixture export, selected runtime tests and binary pinning. Exact argv
and exit receipts are `unnest-repair-command-exits.json` (all six exit 0).

| Check | Actual result | Receipt |
| --- | --- | --- |
| `scripts/cargo-dev test --locked -p arroyo-planner -p arroyo-worker --lib local --no-fail-fast -- --test-threads=1` | exit 0; planner 12/12, worker 3/3 | `local-unnest-repair.log` |
| Same packages/flags, selector `continuous_merge` | exit 0; planner 24/24, worker 0 selected | `continuous-merge.log` |
| Same packages/flags, selector `json_table` | exit 0; planner 9 pass/1 ignored, worker 0 pass/5 ignored | `json-table-final.log` |
| Worker library selector `state_table`, locked/no-fail-fast/one thread | exit 0; 29/29 | `state-table-final.log` |
| Current-source JSON_TABLE fixture exporter, pinned planner binary in dev image | exit 0; 1/1 | `json-fixture-export-unnest-repair.log` |
| Five selected decoded JSON_TABLE worker tests, pinned worker binary in dev image (`json_table --ignored --test-threads=1`) | exit 0; 5/5 | `json-worker-ignored-unnest-repair.log` |
| `scripts/cargo-dev check --locked -p arroyo-planner -p arroyo-worker` | exit 0 | `check-unnest-repair.log` |
| `scripts/preflight-clippy.sh -p arroyo-planner -p arroyo-worker` | exit 0 | `clippy-unnest-repair.log` |
| `scripts/cargo-dev fmt --all -- --check` | exit 0 | `fmt-check-unnest-repair.log` |

Neighboring continuous-MERGE/state-table/normal JSON_TABLE checks preceded the final
UNNEST mode split. The final split reran its affected local and decoded JSON_TABLE
paths plus check/strict Clippy/formatting; unchanged neighbors were not repeated.
Independent split source review approved all authored files/hunks with explicit
authorship exclusions, followed by fresh approval of the final UNNEST mode repair.
Written canonical verdicts are preserved as `final-scoped-review.md`; final staged
file hashes are `final-source-receipt.json`. Source-bound fixtures and exact test
binaries remain outside Cargo caches under the evidence directory.

Final planner test executable SHA256:
`e234a27d969ab3dcb35feed1be83905062d77318c38e37ca1cb5edb76dc6c19e`.
Final worker test executable SHA256:
`67bd5e5014e1122602c7e6191a45109a62614f12e7aff2fe092f20779291af3a`.

Publication and current-head PR CI remain separate coordinator gates.
