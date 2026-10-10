# STR-56 SQL/JSON validation

Worktree: `str-56-delivery-20261010`, branch
`jason/str-56-sql-json-delivery-20261010`, fetched base
`48ceb3fee902d80d012be3de3320b018e3418364`; rescued Phase B `db54dad8`.
Earlier resume/original worktrees and evidence remain preserved.
Instructions: `AGENTS.md`, `.claude/build-test.md`,
`docs/build-qa-troubleshooting.md`, `docs/milestone-3-status.md`, and current
Trakkt STR-56 description/comments. Current user policy removes automatic
elapsed-time deadlines and review signing.

## Acceptance fixtures and independent expectations

`scripts/fixtures/sql-json/ticket-projection.sql` is the exact complete SELECT
from the ticket, including its VALUES input. The opt-in planner test
`sql_json_complete_select_worker_fixture` uses the normal Arroyo parser,
SQL/JSON lowering and native relational/physical planning. Generic physical
protobuf does not serialize MemorySourceConfig; the test captures that native
planner-produced VALUES RecordBatch as IPC and replaces only that bounded
source leaf with the existing ArroyoMemExec worker-input placeholder. It does
not recreate VALUES rows or projection expressions. The complete projection
is physically serialized; `smoke_sql_json.rs::sql_json_complete_projection_worker`
decodes it through production StatelessPhysicalExecutor and worker registry,
then executes the complete query over that planner-produced source batch.
All eight Arrow field types and one row are asserted. Expected output is
independently declared: p1, Boolean TRUE, SQL NULL name, Float64 0.0, Boolean
FALSE, `a@example.test`, a nested discord account object, and coherent
source/medium/accounts object. Serialized numbers/Booleans retain their types;
JSON documents are compared structurally. This is worker physical execution
through a test-only bounded VALUES input adapter, distinct from direct Engine
connector execution of a literal relation or hand-built expressions.

`scripts/test-sql-json.py` declares 15 ordered event outcomes, including every
old/new retained name, VARCHAR document, presence/null test, scalar score and
Boolean, zero/one/two identifier matches, malformed/SQL-null input, blank,
escaped text, false/zero, object/array scalar error branches, empty/nested/null
constructors and quoted versus FORMAT JSON values. Missing name preserves the
retained scalar; present JSON null makes the expression SQL NULL. Each initial
and recovered output row is compared with that declared oracle. The committed
four-row prefix is preserved and compared separately. Matrix: memory/RocksDB,
source batch settings 1/8, controller/leader checkpoint publication, fresh
worker construction from the committed checkpoint.

The rich scalar fixture explicitly uses a 1 MiB working-event allowance to
cover accumulated conservative reservations for multiple JSON invocations.
Production defaults remain unchanged. Oversized worker tests use their own
small allowances; their expected failures are not relaxed by matrix settings.

`state_table.rs::planned_sql_json_merge_*` parses actual MERGE SQL, decodes
planner-emitted fused worker plans, uses typed memory/RocksDB state, and tests
failure/cancellation before commit. Decoded keys, predicates and assignments
(including nested expressions) use the owner’s existing shared admission guard.
Tests assert zero kernel invocations when JSON admission fails, and exactly one
invocation before the second nested extraction exhausts the cumulative allowance. A distinct successful change is staged
first; oversized parsing, expansion and construction then fail. Discarding the
scope and admitted pending-output queue must leave the stored row unchanged,
release complete decoded/write resource pools and allow retry. Kernels execute
synchronously: cancellation is tested after expression execution and before
commit/output delivery, rather than claiming an interruption inside parsing.

## Resource repairs and supported path contract

Path appends check the 1024-item limit before each insertion. Lax predicate
subjects use the same bounded member traversal and one-level array comparison.
keyvalue IDs identify owning objects, rather than individual pairs. The accepted
filter RHS subset is strings and JSON null; numeric/Boolean RHS diagnostics
replace incorrect extra-dialect numeric comparisons. Parsed documents,
construction metadata and compiled paths have invocation-scoped ownership;
there are no newly retained process/UDF caches.

UTF8, LargeUtf8 and Utf8View scalar/array operands share the character contract.
View input buffers remain owned by the already admitted batch; every visible
value byte, including repeated views, is charged before kernel materialization.

Working-event admission reserves before allocation: 64 bytes per source byte
for parsed Value/vector/string/BTreeMap storage; 8 times PathItem size per byte
for two outer and two predicate-subject vectors with capacity rounding; 64 bytes
per source byte for the evaluation-local object identity hash table; 32 bytes
per source byte for conversion, escaping, serialization capacity and Arrow
copies; and a 1024-byte fixed allowance. Every argument contributes, including
constructor metadata and FORMAT JSON inputs. Arithmetic overflow is an execution
resource failure. JSON_OBJECT includes its closing brace in the output bound.

## Source-bound results

Current-source execution passed in the delivery image
`a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`.
The complete SELECT and eight Engine recovery cases passed on the round-four
patch SHA256 `04b84c7438adc6fd632a7e9ba28ad28eafc21eaafe5f8031537fcd05b8f81fca`.
Final follow-up changes only narrow private decode visibility and replace
a test Arc initializer; runtime behavior is unchanged. Final narrow gates
and their source receipt are recorded below.
The initial foreground projection attempt (`scripts/cargo-dev test --locked
-p arroyo-sql-testing sql_json_complete_projection_worker -- --test-threads=1
--nocapture`) exited 101: inserting the exact SELECT directly into an inferred
sink failed planning with `Inconsistent data length across values list: got 2
values in row 0 but expected 8`. Its full build/failure log is
`/home/jason/qa-evidence/str56-20261010/initial-projection-attempt.log`.
Pinned upstream DataFusion `916b45f5`, `datafusion/sql/src/values.rs:47–50`,
applies `PlannerContext::table_schema()` (the eight-column INSERT sink) to an
inner two-column VALUES relation. No upstream/parser cache code was patched.
An outer one-row pulse CROSS JOIN was considered; existing JoinRewriter
requires an equijoin for non-windowed inputs and two timestamp fields, so it
would introduce unrelated graph support requirements. It was not executed.
The physical worker test uses the existing bounded input adapter instead;
no VALUES planner/runtime architecture or public API was added.
Exact command/result receipts and preserved executable hashes are recorded
under `/home/jason/qa-evidence/str56-delivery-20261010/`.
The delivery run uses the updated cargo-dev wrapper with prebuilt RocksDB,
incremental dev/test profiles, and the shared original-checkout target.
Planner and SQL-testing executables are built through cargo-dev and pinned
outside the target cache before runtime-only execution in the same queue reservation.

| Check | Result |
| --- | --- |
| Complete ticket SELECT planner fixture and worker physical execution | pass: one test each, eight types and independent serialized oracle |
| Eight MERGE checkpoint/recovery configurations | pass: memory/RocksDB × batches 1/8 × controller/leader; 15 initial/recovered rows and four-row prefix each |
| Parsed MERGE atomic error/cancellation memory/RocksDB | pass: shared admission before invocation, nested cumulative exhaustion, rollback, permits and retry |
| Focused planner/path/kernel/codec tests | pass: 57 passed, one opt-in test ignored and executed separately |
| Focused worker JSON tests | pass: six tests, zero failures |
| Locked affected-crate check | pass: exit 0 |
| Required affected-crate preflight Clippy | pass: exit 0 |
| Workspace formatting | pass: exit 0 |

Commands are preserved in `*.command` with matching `*.exit` and `*.log` under
`/home/jason/qa-evidence/str56-delivery-20261010/`. The planner/worker focused tests
use `test --locked -p ... sql_json -- --test-threads=1` (worker also `--lib`);
the check names planner, worker and SQL-testing; `scripts/preflight-clippy.sh`
uses its required locked all-feature/all-target CI flags for those three crates.
Runtime receipts, fixture hashes, plan debug output, IPC input, serialized row,
and every matrix capture/run log are preserved under `round4/`. The executable
hashes there refer to pinned binaries outside Cargo’s target cache. Final source
receipt: code-only patch SHA256 `34b10b440dd71d8c8e24c7542c65367324ccd06b39395dc4229c5b9f25d0aeee` (`code.patch`, base/head receipt `head.txt`). The final code differs from the round-four runtime-tested code only in private
decode visibility and test-only Arc initialization; final worker tests, check,
Clippy and formatting passed after those changes. The delivery evidence document
and coordinator ledger are documentation updates after those source-bound gates.

Earlier failed/invalid receipts are retained separately. The first delivery
runtime container omitted stdin forwarding: it exited zero without executing
tests and is explicitly invalid evidence (`runtime-initial-harness-note.txt`).
Corrected execution forwards stdin and checks fixture artifacts, executed test
counts and all eight matrix results before reporting success. The initial atomic
fixtures failed at table construction because 1 MiB pools could not cover the
configured scan headroom (1,578,344 bytes) and queued-write reservation (over
1.25 MiB). Both fixture pools are now 2 MiB; table limits and production defaults
remain unchanged. Oversized JSON fixture event allowances are 128 KiB so input
and projection buffers fit, while conservative JSON admission still fails before
invocation; expansion/conversion cases retain their separate 4 MiB allowance.
No rollback, output or resource-release assertion was weakened.

Round two exposed missing Utf8View character support and the existing unguarded
MERGE decode boundary; both established contracts are now repaired. Round three
failed on cached RPC generated fields absent from the current protobuf. Actual
compiler OUT_DIR, source/generated hashes and stale output are preserved. The
round-four queued `scripts/cargo-dev clean --locked -p arroyo-rpc` exited zero
before rebuilding/testing in the same reservation; no other package or full target
was cleaned. The same two demonstrated generated fields were present again
at the final narrow batch’s queue acquisition; its conditional RPC-only clean
also passed (`codegen-final-clean.log`/`.exit`) in that reservation. STR-73 tracks this shared-target code-generation issue separately.

Independent review and PR CI are delivery gates owned by the coordinator.
STR-57 JSON_TABLE and STR-66 JSON_OBJECTAGG are separate tickets; this evidence
does not qualify those features or the composed external application catalog.

STR-70 tracks the non-blocking parser-fork enum-name lint cleanup; it does not block STR-56 or change SQL semantics.

Final publication integrates main `8cbd443d` by clean rebase. Its delta from
validated base `48ceb3fe` contains only preservation tooling and build guidance;
no engine source changed. Applicable AGENTS/build guidance was reread. Independent
review approved publication after auditing the final source and receipts.
