# STR-147: pinned native array_concat empty selection repair

Source: https://github.com/ArroyoSystems/arrow-datafusion at commit
`916b45f5c28d94765ae4a6393c5e126b2ea55e1c`, package
`datafusion-functions-nested` 48.0.1, directory `datafusion/functions-nested`.
Apache-2.0 LICENSE.txt, NOTICE.txt, README and upstream sources are preserved.
Cargo workspace inheritance is expanded to the same package, lint and dependency
settings. DataFusion dependencies retain their existing locked git source; no
version upgrade is intended. As in the aggregate-common patch, Criterion benchmark
sources are retained but targets/dependency are disabled to avoid adding a separate
benchmark dependency graph; existing Rand tests remain enabled.

Production repair is confined to `src/concat.rs`, `concat_internal`: when the
collected child arrays are empty, construct an empty array with the already
resolved child datatype instead of calling Arrow `concat` with no arrays.
Existing offsets and validity are retained. This handles zero-row selections
(native physical CASE evaluates unselected branches with zero selected rows)
and nonempty all-null typed list rows. Nonempty child concatenation is unchanged.
Native `ArrayConcat` registration/type identity, coercion, dimensional alignment
and List/LargeList dispatch remain unchanged. No application special case,
planner workaround, new capability, state backend or RocksDB scan is introduced.

Regression tests cover zero-row List/LargeList values including null/nested child
types, typed lists with untyped Null arguments, all-null rows, nonempty nullable
rows, ordering, empty rows and zero-argument rejection. The planner regression in
`crates/arroyo-planner/src/test/local_subqueries.rs` executes native nested CASE
through the normal logical/physical planning, codec and local execution path.
Validation commands/results and actual integration evidence are recorded on
STR-147 by the coordinating agent; this receipt does not claim unrun checks.

`upstream-source-sha256.json` records every original file hash before patching.
Workspace rustfmt normalizes copied Rust source layout; outside `src/concat.rs`
those are formatting-only changes. No upstream files are omitted. New tests call
raw kernel with typed-first/Null-second inputs for dimension alignment: the raw
helper assumes a common child type, while public SQL coercion supplies that
common base type before invoking it. This patch does not extend raw uncoerced
mixed-child-type semantics. A separate direct physical CASE test in
`crates/arroyo-planner/src/local_case_tests.rs` prevents optimizer folding from
removing the zero-selected-row branch under test.

The SQL regression preserves the native VALUES fixture rows and SQL-planned
projection, adapting only the in-memory fixture source to the existing
`ArroyoMemExec`/`LockedBatchVec` worker input path before full plan codec
roundtrip. This follows existing SQL/JSON and LIKE_REGEX fixture tests; it does
not add serialization support for DataFusion's test-only memory source. The
first attempted direct memory-source roundtrip failed before kernel execution
and is retained in STR-147's validation evidence.
