# STR-147: native array concatenation on empty selections

## Problem and repair

The existing SQL `CASE WHEN predicate THEN ARRAY['created'] ELSE
array_concat(ARRAY[key], ARRAY[key]) END` must work when every row selects THEN.
DataFusion evaluates the unused ELSE over a zero-row batch. The pinned native
array_concat kernel built no child arrays and passed an empty input slice to
Arrow concat, which fails. The same problem affects typed all-null list rows.

This repairs the native kernel: when the child-chunk collection is empty,
construct an empty child array with the already resolved element datatype.
Keep existing offsets, row validity and List/LargeList dispatch; use the original
concat path for nonempty children. No application SQL, UDF, function wrapper,
clock, state protocol or backend semantics are introduced.

The exact pinned dependency is vendored through the repository's existing
DataFusion git-source patch mechanism. `vendor/datafusion-functions-nested/STREAMR.md`
records upstream source and original file hashes, manifest flattening and local
changes; original licenses are retained. Package version and native concrete
ArrayConcat identity remain unchanged, including the state-table fusion whitelist.
No dependency upgrade is included. Public argument coercion remains unchanged.

## Source and workflow

- Fetched base: `a5d28ad868f8e64cd3d2daa158b12e11b52e3c03`.
- Worktree: `/home/jason/repos/streamr-worktrees/native-empty-array-20261023`.
- Instructions: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, current
  milestone status/work-plan and STR-147; in-flight check CLEAR.
- Original dirty checkout and prior evidence preserved.
- Build purpose: native expression correctness and application recovery;
  normal dev/test, matching precompiled RocksDB in immutable image
  `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
  Maintained scripts and shared queue only.
- Evidence root: `/home/jason/qa-evidence/str147-empty-array-20261023/`.

## Baseline and regressions

Saved current-source failure (not repeated):
`/home/jason/qa-evidence/arc18-readiness-20261023/empty-case-array-1/` and
`publication-metadata-1/`. Actual public Kafka execution reports:
`Compute error: concat requires input of at least one array`.

Application-owned SQL and literal fixtures remain in Arcstream PR32. Streamr's
regressions use generic keys, lists and selected branches; no Arcstream schema
or lifecycle rule is embedded in this engine.

The complete nested-function library suite passes13/13, including four new
kernel tests for zero-row List/LargeList/nested/Null values, all-null rows,
nonempty order/null handling and rejection of zero arguments. Planner tests
include an explicit physical CASE that cannot be optimized away and a nested
CASE plan/codec execution case. Remaining maintained gates and actual unchanged
application revalidation are recorded below when completed.

## Review, resources and release scope

Independent source review approves the generic repair. All37 upstream source
hashes match the exact dependency; other copied Rust changes are documented
formatting only. Written delivery approval still requires actual validation/CI.

No RocksDB scan is new or changed. The fix affects construction of a native
expression result only; state ownership, limits, checkpoint/recovery and queue
behavior remain intact. Reuse completed engine capacity qualification; no new
throughput or production capacity claim.

STR-148 and STR-149 track separate SESSION/publication-clock investigations.
The user explicitly directed that STR work needs tickets but does not
necessarily block Arcstream release. Neither has an automatic release-blocking
relation. Required release functionality must drive application gates; current
strict decoder fields are not proof every field must ship in the first release.
No consumer field is silently filled or dropped by this patch. No deployment,
traffic switch or production data modification is included.

## Maintained source gates

- Full native nested-function library:13passed/0failed.
- Workspace format check and locked patched dependency/planner check:pass.
- Planner physical CASE and worker codec tests:2passed/0failed.
- Strict Clippy, both packages/allfeatures/alltargets:pass.
- Original planner test attempt had1pass/1fail because its literal VALUES
  fixture materialized an unsupported in-memory source for plan serialization.
  The corrected fixture uses the existing ArroyoMemExec worker-input adapter,
  preserving exact source rows, planned CASE expressions and codec round-trip.
  No production codec change or error waiver was added. Both attempts remain
  in `gates.json`/`planner-tests.log` and `gates-2.json`/matching logs.
- Independent material test-helper review approves that correction.

The user approved omitting unused publication metadata from Arcstream's first
native release. STR-147 is therefore a separate product fix, **not a blocker**
for that22-field release path. Application kernel regression validation and
required final-head CI remain pending; this report does not claim release.
