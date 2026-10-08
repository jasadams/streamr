# Typed result composition and optional emission observations

The ordinary SQL in `scripts/native-typed-result-composition.sql` assembles a
changing scalar COUNT and a typed top-five array into one result without an
updating join. Per-item COUNT feeds ordered flat-STRUCT ARRAY_AGG and array_slice;
UNION ALL pads absent branch columns with typed NULLs. An outer MAX selects the
sole current scalar contribution, and ordered FIRST_VALUE/FILTER selects the
sole current nonnull array. COALESCE supplies a typed empty array when needed.
No application schema, SQL extension or operator emission policy is added.

The 23-event generic fixture checkpoints after event 21. At checkpoint `b` has
total 2 and `y:1`; `c` has total 9 and `f:2,g:2,a:1,b:1,c:1`. Final `b` remains
unchanged and `c` has total 7 and `a:1,b:1,c:1,d:1,e:1`; deleted `a` is absent.
The null member contributes to the scalar total and is filtered from ranking.
Eight diagnostic captures on the previously built binary passed at
`target/native-typed-result-composition-proposed/comparisons.json`. They do not
qualify subsequent Rust changes or this new portable runner. Fresh combined
build/runtime evidence is required before extending that source claim.

The query bounds output length only. ARRAY_AGG still retains/materializes all
distinct members before slicing, under configured collection limits. Branches
may become visible separately; neither the fixture nor comparator asserts
cross-branch atomicity, complete application parity or bounded hot-key ranking.

## Portable caller-declared capture

Inside the prescribed development container, using the current SQL-test binary:

```sh
python3 scripts/native_typed_result_composition.py /app/target/typed-composition-fixture
python3 scripts/test-native-result-manifest.py /app/target/typed-composition-fixture/manifest.json BINARY /app/target/typed-composition-fresh --source-evidence BUILD_SOURCE_EVIDENCE
```

The runner serializes memory/RocksDB, controller/leader and source batches 1/8.
Repeated `--backend`, `--protocol`, `--batch` restrict the matrix. Result case
directories must be new. The manifest names caller-owned `query`, `input` and
`expected` files relative to itself, plus `checkpoint_input_rows`, optional
positive `max_capture_rows` and `timeout_seconds`, and explicit `test_limits`.
SQL connector paths use already quoted `{{INPUT}}`/`{{OUTPUT}}` placeholders.
Four-owner resource overrides are fixture declarations; production defaults
are unchanged. This capture requires at least one emitted record at checkpoint,
although its materialized checkpoint state may be empty after deletion.

The expected JSON declares `key_fields`, `prefix_states` (arrays of complete
materialized rows), `checkpoint` and `final`. Optional `intermediate_values`
maps non-key fields to explicit typed alternatives such as NULL or an empty
array during branch assembly. Every captured field must match a declared
same-key branch-prefix value or alternative, preserving nested JSON types and
array order. Every CDC before-image must equal the preceding emitted row;
operation/image consistency and exact field/key sets are mandatory. Final
uninterrupted, committed checkpoint and fresh-worker materializations must
match complete expected rows. Intermediate columns are checked independently;
the comparator does not prove a shared source-prefix transaction or ordering
between branches. Caller fixtures and expected values remain caller-owned.

Per-case inventories hash the manifest, original/rendered SQL, input, expected
values, runner and binary before execution and verify they stay unchanged.
An optional source-evidence file is pinned by hash; a binary hash alone does
not establish which source was compiled. Exact environment, passing capture
markers, CDC files, comparison hashes and child peak RSS are retained. RSS is
reported without making a capacity claim. The shared capture cancels/recreates
workers inside one process; process-kill, remote/sink failure and live gates
remain separate.

## Optional initial-phase source schedule

`STREAMR_CAPTURE_INITIAL_SCHEDULE` points to an absolute caller-owned JSON file:

```json
{"max_output_bytes":65536,"steps":[
  {"kind":"observe","at_ms":100,"label":"early"},
  {"kind":"advance","at_ms":4100,"source_row_target":2},
  {"kind":"observe","at_ms":4900,"label":"before_tick"},
  {"kind":"observe","at_ms":5300,"label":"after_tick"},
  {"kind":"observe","at_ms":9400,"label":"after_pending_deadline"}
]}
```

This test-only hook requires source batch target 1, a nonempty control-waiting
single-file source and no `CaptureIdle` hold. At most 64 steps span at most
120 seconds within the runtime timeout; each observation has a declared
1-byte–4-MiB limit. Source targets increase and cannot exceed available rows.
Labels are unique bounded ASCII names. The first row is released automatically
by the existing connector. The monotonic epoch is taken immediately before
`Engine.start`; startup latency and initial source admission are not controlled.
`source_row_target`/`noops_total` record successful release controls, not an
acknowledgement that downstream operators have processed every row. Callers
must compare observed values and measured times rather than equating release
with execution. Late scheduling is recorded, never normalized to the target.

Observations immediately read the current bounded complete JSONL prefix,
including zero rows, without waiting for a value or minimum count. A partial
last record is excluded. Logs report scheduled time, sampling-start time and
completion time; concurrent file reads establish an observation interval,
not an atomic instant. Snapshots preserve every earlier complete byte prefix.
After the last observation, ordinary source advancement/EOF and all existing
checkpoint/recovery assertions proceed unchanged. The schedule applies only
to uninterrupted initial capture; it does not claim scheduled restore behavior
or a checkpoint at an exact wall time. No production clock or timer changes.

Native aggregates currently flush on periodic operator ticks and checkpoints;
the [upstream documentation](https://doc.arroyo.dev/sql/updating-tables/) describes
periodic buffering. Setting a five-second flush interval does not establish
immediate creation or a per-key deadline five seconds after its first pending
change. A later change at 4.1 seconds can flush at a startup-aligned tick near
5 seconds, rather than its pending deadline near 9.1 seconds. This schedule
permits an actual diagnostic observation of that difference. The subsequent
run authorized the change near 4.1 seconds and observed its output by 5.3
seconds, absent at 4.9 seconds, as recorded in the milestone validation
evidence. The optional immediate-first
and per-group deadline policy is deferred post-MVP in STR-44; milestone 3
retains existing periodic flushing. Any new contract requires design approval.
