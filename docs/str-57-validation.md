# STR-57 JSON_TABLE validation

Base `origin/main`: `299f17f37fb0eb28709bb4bcc50e69743fa7b131`.
Branch: `jason/str-57-json-table-20261011`. Instructions: `AGENTS.md`,
`.claude/build-test.md`, `docs/milestone-3-status.md`,
`docs/milestone-3-work-plan.md`, current STR-57 description. The current MVP
instruction removes graph/operator admission; older ticket wording does not
restore it. STR-58 composed FULL OUTER JOIN/atomic patch MERGE remains deferred.

JSON_TABLE becomes a typed lateral derived relation using existing native
UnnestExec, not a redundant custom physical node. Relation binding preserves
ordinary qualified/unqualified references and alias column lists. Row and column
kernels share STR-56 compiled paths, conversion/default and JSON serialization.
No retained streaming history, persistent namespace or new admission pool exists.
Unsupported optional column clauses produce explicit planner errors.

Bounds reuse `MAX_DOCUMENT_BYTES` (16 MiB), `MAX_PATH_ITEMS` (1024) and
`MAX_OUTPUT_BYTES` (16 MiB). The shared evaluator bounds expansion; serialization
checks per-event bytes before buffer extension. Batch size does not combine that
budget. Resource exhaustion is an error; malformed/SQL-null row documents yield
empty lists. Ordinary multi-column projections can multiply output bytes; this
feature does not add a graph/operator byte reservation. Native row queue
backpressure remains unchanged.

The maintained parser fork is pinned in both Cargo patches/lock entries to
`bc7f08176c80f8fae443e69add9489b7627cbacb`, independently reviewed before root
published it. It adds FORMAT JSON AST preservation. Dependency tests passed
23 Arroyo and 93 MySQL cases with serde/visitor on the host (pure Rust parser);
these are dependency evidence, not Streamr container gates.

Actual native DataFusion serialization handles Unnest before extension encoding
(pinned upstream `physical_plan/mod.rs:509`; encode 2725, decode 1751), preserving
indices, options and full schema. All current Streamr callers use that serializer.
The obsolete custom Unnest message/codec was removed. Worker post-decode validates
full input/output schema, index, supported options and UDF argument/path/return
metadata; malformed plans fail before invocation. Tests mutate the actual native
wire representation, not a test-only codec path.

Source-bound regression tracing also found two preexisting UnnestRewriter issues:
a hardcoded `__unnested` private name collided with user fields, and upstream
child traversal preserved cached parent Projection/SubqueryAlias schemas
(`logical_plan/tree_node.rs:77`) after nullability refinement. The repair chooses
a collision-free private name and re-infers those schemas using native
constructors. User field names are preserved. Existing ordinary UNNEST and a
new two-name collision regression pass. Fixture physical planning uses the
production single-partition settings (`lib.rs:908`), avoiding fixture-created
RoundRobin repartition plans incompatible with per-batch executor reuse.

All engine gates ran foreground in the required Rust 1.96 container, serialized
through the shared machine queue/target. Immutable image:
`e67afacf02e9b93cd67e603af190b6a68f67d9efab9c857441162c1359c14382`.
Durable evidence root:
`/home/jason/repos/streamr-validation/str-57-json-table-20261011`.
It contains exact runner scripts, ticket/review receipts, `gates/` logs,
`source-final.json`, `fixtures-sha256.json`, `test-binaries.json`, `image-id.txt`,
fixture SQL/plans/input IPC/protos, and pinned binaries outside regenerable Cargo
caches. Pinning/export/worker execution shared one queue reservation.

| Final selected gate | Actual result |
| --- | --- |
| `scripts/cargo-dev update -p sqlparser@0.55.0` | exit 0; unrelated Windows lock reassignments restored from base |
| `scripts/cargo-dev fmt --all` | exit 0 |
| `scripts/cargo-dev check --locked -p arroyo-rpc -p arroyo-planner -p arroyo-worker` | exit 0, repair2 4m12s |
| `scripts/preflight-clippy.sh -p arroyo-rpc -p arroyo-planner -p arroyo-worker` | exit 0, final 18.88s |
| `scripts/cargo-dev test --locked -p arroyo-planner json_table --no-fail-fast -- --nocapture` | exit 0, 8 passed / 0 failed / 1 ignored exporter |
| `scripts/cargo-dev test --locked -p arroyo-planner sql_json --no-fail-fast -- --nocapture` | exit 0, 60 passed / 0 failed / 1 ignored exporter |
| `scripts/cargo-dev test --locked -p arroyo-planner test::plan_tests::for_each_file::path_52_src_test_queries_unnest_sql --no-fail-fast -- --nocapture` | exit 0, 1 passed |
| `scripts/cargo-dev test --locked -p arroyo-planner -p arroyo-worker --lib --no-run --no-fail-fast --message-format=json` | exit 0; current test executables pinned |
| Pinned planner `json_table_complete_worker_fixtures --ignored --nocapture --test-threads=1` | exit 0, 1 passed; 7 complete fixtures |
| Pinned worker `json_table --ignored --nocapture --test-threads=1` | exit 0, 5 passed / 0 failed, 2.75s |

Pinned container calls use `podman run --rm -w /app`, workspace/evidence mounts
and `STREAMR_JSON_TABLE_FIXTURE_DIR=/evidence/fixtures`; complete literal commands
are in `str57-runtime-validation.sh`. Planner SHA256:
`affdc365ef279b3467f72bbb3dedb853549418eca515c1b83de366464b765bf2`.
Final worker SHA256:
`fd08f42afc09ab7875c201b8a8bc0f0cc249de94111899b5b0f84c60fd5d101b`.

Exact ticket standalone tags/accounts and correlated p1/p2 SQL are exported and
executed through worker decode with independent expected values. Variable-input
`tags`, `accounts`, `correlated` yield respectively 24, 16, 8 rows from eight
source rows at both batches 1 and 8. `collision` preserves user `__unnested`;
`presence` asserts missing versus present-null, false/scalar/empty/malformed,
SQL NULL and null parent. Runtime replacements additionally assert nested
objects/arrays/escaping FORMAT JSON, incompatible Boolean NULL defaults,
false/null members, SQL-null row input, no phantom empty rows, near-byte-limit
batches 1/8, expansion errors and reuse after error/cancellation. Forty UDF
metadata mutations and ten native Unnest metadata/options mutations reject
before execution. No checkpoint evidence is fabricated for invocation-local state.

Historical frozen inventories remain preserved: initial check passed, initial
strictClippy failed `Value.repeat` Copy requirement, initial planner 3 passed /
1 ignored. Repair1 check passed; Clippy failed sole useless_format; planner
5 passed / 1 failed / 1 ignored; shared SQL/JSON 60 passed / 1 ignored. Repair1
export failed existing name collision; all five runtime tests failed (three
fixture repartition panics, valid EXISTS schema mismatch, incomplete native
mutation traversal). Complete failed pinned evidence is in `repair1/`.
Repair2 planner/export/runtime all passed; wrapper exit 1 was solely
clone_on_copy in a test mutation. Final one-line Copy repair passed strictClippy
and repinned/reexecuted all five worker tests; unchanged planner gates were not
repeated. Repair2 artifacts are in `repair2/`.

Independent initial review requested seven major repairs; repaired source was
approved, then material native-codec/schema repairs received a fresh APPROVED
verdict (`str57-streamr-review-repair2.md`). Root owns final focused review,
publication and full PR CI; those are separate from these completed local gates.
