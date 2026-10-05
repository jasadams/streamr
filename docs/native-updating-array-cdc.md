# Generic updating ARRAY_AGG qualification fixture

This fixture is independent of any application schema. `native-updating-array-cdc.sql` reads a primary-keyed single-file Debezium source (`row_id`, `k`, `v`, `position`) and groups by `k`. It emits `COUNT(*)`, ordered `ARRAY_AGG(v)`, ordered `ARRAY_AGG(v) FILTER (WHERE v IS NOT NULL)`, and ordered `ARRAY_AGG(named_struct(...))` to a Debezium sink. DataFusion documents `named_struct` as a scalar constructor; Arroyo documents `ARRAY_AGG` ordering; the fork already uses `FILTER` on aggregates. The first actual memory/controller/batch-1 attempt on worker source `8a11afc1…` planned and started, then failed the collection budget guard. That failure is retained at `target/native-updating-array-cdc-indexed/memory-controller-batch1/capture.log`; that attempt did not pass runtime/recovery qualification. The latest eight-case result is recorded below. Default `ARRAY_AGG` null retention is corroborated by the current native unit `native_array_index_preserves_order_nulls_duplicates_filter_and_retractions`; the latest runtime matrix now checks the complete SQL values as well.

The 12 source envelopes have exact `before`/`after`/`op` images and `ts_ms`. Rows 1–2 insert duplicate `x` values for key `a` at distinct positions. Row 5 updates one duplicate to `z`; row 6 deletes the other, so exactly one occurrence disappears. Row 7 inserts a null argument. The checkpoint is after row 8, with `a` ordered `[null, q, z]` and `b` ordered `[y, null]`. Row 9 updates `q` to `x`; rows 10–12 retract the remaining `a` members, ending with a deletion of group `a`. Final `b` retains `[y, null]`; its filtered array is `[y]`. Struct records carry `row_id`, `sort_pos`, and nullable `label` in the same order. Null-only groups are avoided so the filtered aggregate never has an ambiguous empty-group result.

Run `python3 native_updating_array_cdc.py HOST_DIR --runtime-directory /app/target/...` to prepare eight cases: memory/Rocks × controller/leader × source batch 1/8. The host and container directories must be the same bind-mounted files. Each manifest case gives exact env and `query.sql`; the shared `external_sql_checkpoint_capture` test binary supplies execution. Save its output as `capture.log`, then run `python3 native_updating_array_cdc.py HOST_DIR --compare`. The comparator requires typed complete JSONL, duplicate-field rejection, exact CDC before images, source-prefix-reachable per-key values (allowing coalescing and independent key flush order), initial final state, recovered committed checkpoint-8 state, and recovered final state. It checks file/template/oracle hashes, rendered query and env mapping. A pass would qualify this generic SQL path under the eight configurations only, not arbitrary collection sizes or workloads.

`HOST_DIR/oversize` is a **separate expected failure** with one 64-KiB member against the test harness's 32-KiB native aggregate value budget. Run it separately, save the test log as `capture.log` and the process exit status as decimal `exit-code.txt`, then use `--compare-oversize`. The comparator requires a nonzero exit and the explicit native collection budget diagnostic. This case must never be counted as a successful output capture. Both memory and Rocks adapters use the same SQL and aggregate-state limits; this single negative case tests the declared error path, not a capacity matrix.

Evidence for source shape: `crates/arroyo-sql-testing/src/test/queries/debezium_agg.sql` and `inputs/aggregate_updates.json`; required create/update/delete images are validated in `crates/arroyo-types/src/lib.rs:248-268`. Evidence for native null/duplicate behavior and budget guard is in `crates/arroyo-worker/src/arrow/incremental_aggregator.rs` native array unit tests and `native_collection_budget`. No engine code or public SQL interface is changed by this fixture.

## Source-qualified diagnostic result

The constructor repair at worker SHA-256
`8a11afc1c05fec233190269ec4cc06be356093e24f0675960eb393c74a4719d3`
passed Bookworm formatting, workspace all-target check, strict Clippy, 25
library suites (623 passed, four ignored), and full workspace build.
The tested SQL executable was SHA-256
`7a843dfc70f7f3f692f7566d3a0a9b93287d4ecfeb975aa450a7b6e9c33c49d5`.
A separate external application-owned already-ranked TEXT[] query passed
all eight configurations and strict comparisons at
`target/native-profile-ranked-array-indexed/comparisons.json`. That is array
assembly evidence, not typed-struct/retraction qualification for this fixture
and not ranking or full profile parity. That diagnostic was followed by the repairs and qualification below;
the limits and value oracle were unchanged.

## Typed CDC value failure after the schema repair

The audited flat-STRUCT array codec and declared CDC schema repair passed all
five Bookworm gates with 626 library tests (four ignored). The tested worker
was `7c577ccb…`, planner physical source `fd45be72…`, and SQL executable
`9d264ac90e250012655c12f72392bd5f54eed1cbeef060238db4c417a39ded22`.
All eight captures in `target/native-updating-array-cdc-declared-schema-fixed`
executed successfully, but the strict comparator rejected a zero-count row with
null arrays after the last member of `a` was deleted. The expected grouped row
must disappear. Capture success therefore does not qualify these values.
This failure was followed by the empty-group and timestamp repairs below; the fixture and value oracle were unchanged.

Updating ARRAY_AGG currently admits audited scalar arguments and one flat
STRUCT layer, including ORDER BY arguments. Nested STRUCT, List/LargeList,
Dictionary and RunEnd arguments are rejected before row decoding. Collection
multiplicity, including duplicate and null occurrences, counts toward the
existing aggregate value and output budgets; DISTINCT does not exempt duplicate
occurrences from those limits. These bounds do not claim arbitrary nested codec
or large/hot collection qualification.

## Empty filtered arrays through standard SQL

`scripts/native_empty_array_cdc.py` saves the separate standard SQL probe and
its strict oracle. Prepare it with `HOST_DIR --runtime-directory /app/target/...`
and compare captures with `HOST_DIR --compare`. Its SQL uses
`COALESCE(ARRAY_AGG(...) FILTER (...), CAST(ARRAY[] AS ...[]))` for TEXT and
flat STRUCT arrays. After one null-valued input, the checkpoint row has count
one and two empty arrays; after the next input, count two and the expected
TEXT/STRUCT members must appear with exact CDC before-images.

All eight cases passed capture and strict comparison on the 626-test source
and executable above. Artifacts are
`target/native-array-empty-coalesce-declared-schema/comparisons.json`.
The same executable passed a fresh external already-ranked TEXT-array matrix
at `target/native-profile-ranked-array-audited/comparisons.json` and all eight
external 14-field profile-core comparisons at
`target/native-profile-core-declared-schema/*/comparison.json`. Those external
fixtures and business oracles remain application-owned; these results do not
qualify ranking, full profile output or lifecycle timing.

The first oversized-value run failed fixture configuration before execution
and is retained under the earlier typed fixture's `oversize/capture.log`.
After supplying the harness's required checkpoint settings, the fixed case
failed explicitly at the native collection budget with process exit 101.
`target/native-updating-array-cdc-oversize-config-fixed/oversize/comparison.json`
records that expected resource failure. It is not a successful output capture.

## Qualified current repair (2026-10-05)

The generic typed CDC fixture passed all eight captures and strict initial,
checkpoint-8 and recovered-final comparisons at
`target/native-updating-array-cdc-timestamp-identities-union-fixed/comparisons.json`.
This includes exact ordered TEXT and flat-STRUCT arrays, nulls, duplicate
occurrences, FILTER, updates, retractions and disappearance of the deleted group.
The separate oversized member failed with exit 101 and the expected native
collection budget diagnostic; its comparison is an expected resource failure,
not a successful capture.

The empty-array COALESCE matrix also passed all eight comparisons at
`target/native-array-empty-coalesce-timestamp-identities-union-fixed/comparisons.json`.
The external already-ranked TEXT-array matrix and external 14-field profile-core
matrix each passed eight captures and their application-owned comparisons under
the corresponding `target/native-profile-*-timestamp-identities-union-fixed`
directories. These results qualify neither lifetime ranking nor full profiles
or lifecycle timing.

All five Bookworm gates passed with 636 library tests (four ignored). Worker
SHA-256: `63c9311ff82ab429bfce84b770857eb1b5f3066b3a8eecc5c79b35936781f12e`.
SQL capture executable SHA-256:
`35dafbefefc91699a486e686615dbbdc580888e6ff4764ad101a5db75471fa2f`.
Each proof directory contains the full planner/worker source hashes and
`source-evidence.json`; the combined run is recorded at
`/tmp/streamr-m3-timestamp-identities-union-fixed-v3-pipeline-results.json`.
The [timestamp regression record](updating-aggregate-timestamp-regression.md)
explains the retained-row identity repair and UNION consolidation. Existing
collection bounds and unsupported nested codecs remain in force.
