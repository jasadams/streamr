# Caller-declared native SESSION qualification

`scripts/test-native-session-manifest.py` supplies additional native SESSION
fixtures and accepts an external manifest. It uses the existing real SQL planner,
configured native SESSION owner and `external_sql_checkpoint_capture`; it does
not simulate SQL or derive expected values from observed output. The coordinating
agent must run the fresh SQL test executable in the required development container.
Preparing a fixture alone is not operator qualification.

The generated fixtures use only opaque keys, event timestamps, ordinal numbers,
numeric values, text attributes and Boolean collection members. Applications supply their own schemas,
queries and independently declared complete expected rows through a manifest.
No external checkout is required.

| Fixture | Input/checkpoint prefix | Complete output oracle | Scope |
| --- | --- | --- | --- |
| `ordered-reuse` | 6 / 4 | 2 rows | Nonempty ordered FIRST/LAST with FILTER and exact ordered integer/text/Boolean arrays, earlier timestamp arriving later, two event-time intervals for one key, both open at checkpoint |
| `closed-reuse` | 6 / 4 | 4 rows, 1 emitted at checkpoint | Exact ordered arrays and metadata after a deleted earlier interval plus open reused-key state and another pending key; watermark 40 equals another session's deadline 40 |
| `continuous` | 9,602 / 4,801 | 1 row | Event spacing 9 seconds with gap 10; last event 86,409 seconds after first; complete scalar/ordered values and open restore |
| `hot` | configurable rows / rows minus one | 1 row | One opaque key with independent payloads every 9 seconds; complete scalar/ordered values and open restore |
| `many` | `2 * keys` / `2 * keys - 1` | `keys` rows | Two interleaved independent payloads per simultaneously open key, all windows/scalars/full first and last payloads before and after restore |

The continuous fixture uses a two-day watermark lag so the first retained row
cannot be discarded under the watermark before the final EOF closes the session.
It probes native retained history beyond the legacy `gap * 100` retention and
24 hours. Native SESSION has no explicit maximum-duration setting. This fixture
neither adds a cap nor assumes one. The closed-reuse fixture uses a direct
event-time watermark and batch target 1. Checkpoint cardinality asserts that
strict equality keeps the deadline-40 session open while the deadline-19 session
has closed. Final rows additionally assert that the retired key's old values are
absent from its new interval.

The existing native equality/bridge and direct late-drop drivers remain separate
prior evidence; this driver does not replace their oracles. Generated fixtures
are candidate qualification cases until real captures pass on the current source.

## Commands

Run cases serially with the same built SQL-testing executable. For example,
inside the development container, replacing `BINARY` with its absolute path:

```sh
python3 scripts/test-native-session-manifest.py /app/target/session-ordered --prepare ordered-reuse --binary BINARY --backend memory --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-closed --prepare closed-reuse --binary BINARY --backend memory --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-continuous --prepare continuous --binary BINARY --backend memory --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-many-small --prepare many --keys 8 --payload-bytes 64 --binary BINARY --backend memory --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-hot-small --prepare hot --rows 8 --payload-bytes 64 --binary BINARY --backend memory --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-hot-65001 --prepare hot --rows 65001 --payload-bytes 8192 --binary BINARY --backend rocksdb
python3 scripts/test-native-session-manifest.py /app/target/session-many-65000 --prepare many --keys 65000 --payload-bytes 4096 --binary BINARY --backend rocksdb
```

Both controller and leader protocols run by default. Ordered/closed reuse also
default to both memory and RocksDB; ordered reuse runs source batches 1 and 8.
`--backend`, `--protocol` and repeatable `--batch-rows` limit or select a matrix.
Capacity fixtures keep RocksDB as their default backend. Omit `--binary` to prepare input/SQL/expected/manifest files only.
Check free disk before the large case: input plus expected payload text already
exceed 1 GiB, before live RocksDB and checkpoint storage. The 129,999-row large
checkpoint has a conservative raw retained-payload floor of 532,475,904 bytes,
a declared estimate above 10 times the 50 MiB executor/backend pool sum;
this estimate alone does not qualify actual retained checkpoint state. The floor counts
source text once per retained input, without multiplying session metadata or
index entries. The whole child SQL-test process has a declared 512 MiB RSS limit.
The hot fixture similarly retains every prefix payload with a watermark lag
longer than the entire generated event-time span. A 65,001-row hot input with
8,192-byte attributes checkpoints 65,000 rows, a 532,480,000-byte raw-payload
floor. This is continuous event-time activity, with no duration cap.
Peak RSS comes from `wait4`, including all worker and checkpoint tasks in that
process. Fixture preparation and Python oracle verification run outside that
child; this is not a measurement of the verifier's memory. Ordered output
attributes compare both independently generated full payloads for every key.
This is stronger than checking a count and the first hot-session payload, but
still does not expose all raw intermediate rows. Preparation therefore also
writes `retained-projection.json` and `expected.retained.jsonl` for the exact
checkpoint prefix. Use the existing `native-checkpoint-inventory.py` and
`verify-retained-checkpoint-rows.py` with the captured owner/table inventory and
row-key prefix `52` to verify actual retained IPC rows and attribute bytes.
The projected original columns deliberately exclude internal routing/time fields;
the verifier validates their unique names, Arrow types and nullability in the
actual IPC schema. Declared floors remain separate from measured payload bytes
in `measurement.json`; payload measurement and the measured 10x verdict remain
unset until that independent proof is run. Total checkpoint bytes and compressed
file sizes do not establish a raw attribute-payload floor.

## Manifest contract

Paths are relative to the manifest, or absolute. SQL uses `{{INPUT}}` and
`{{OUTPUT}}` inside already quoted connector paths; the driver replaces these
with SQL-escaped absolute paths. The query must satisfy the existing harness's
single control-waiting file source, singleton graph and JSON file sink contract.
The driver sets the configured native-windows route, selected memory/RocksDB
backend, protocol and 16 MiB execution pool identically for each run.

```json
{
  "query": "query.sql",
  "input": "input.jsonl",
  "expected": "expected.json",
  "expected_checkpoint": "expected.checkpoint.json",
  "checkpoint_input_rows": 4,
  "checkpoint_output_rows": 0,
  "batch_rows": 1,
  "timeout_seconds": 900,
  "rss_limit_mib": 512,
  "retained_payload_bytes_per_input": 0,
  "retained_payload_proof": ""
}
```

`expected.json` is an array of complete expected output objects.
`expected_checkpoint` names an array of complete committed prefix rows, whose
length must equal `checkpoint_output_rows`. After restore the driver reads the
reported `CAPTURE_RESULT committed_rows`, verifies it against the declaration,
and compares exactly that first portion of the recovered sink file. The sink
preserves committed prefix output before appending resumed suffix output. The comparator
checks field names, exact JSON types and values, and multiplicities; it ignores
only cross-key output order. Caller-owned native array expectations retain their
array order. For a positive independently established equal-size raw payload
floor, set `retained_payload_bytes_per_input`; leave zero otherwise. A positive floor also requires a nonempty `retained_payload_proof` declaration
explaining why every prefix row remains retained in the selected SQL plan.
The generated hot/many fixtures declare equal-size independent attributes,
raw-input ordered final aggregation, a watermark behind all inputs and no
closed prefix sessions. Arbitrary external SQL receives no automatic floor
claim merely because it has zero prefix output. The proof is retained in the
measurement artifact. These fields are declared fixture premises, not measured
live-state sizes; qualification requires auditing the supplied plan and premise.

An optional `idle` object uses the existing finite-source pause probe:
`source_row_target`, `seconds`, `max_output_bytes`, `expected_before` and
`expected_after` (arrays of complete rows). Its source target must exceed the
checkpoint prefix; batch target must be 1; the harness requires a nonempty
before-output observation. Optional `pre_match_pointer` and `pre_match_value`
wait for one declared output value before starting the hold. Initial and restored
before/after captures are compared independently. Pausing the producer leaves
it live and prevents EOF; this establishes the declared no-new-input observation,
not automatic all-idle watermark advancement or processing-time closure.

## Evidence limits and remaining acceptance

The shared capture performs a full stopped checkpoint, cancels workers, constructs
fresh workers, and resumes the source from the committed prefix. It does not
restart the OS process or qualify remote/sink commit failures. The sink file
contains checkpoint-prefix output followed by recovered suffix output. The driver
compares the complete file after restore and its reported committed prefix
against independently declared complete row oracles. This recovers an exact
prefix-value assertion without modifying the shared harness or checkpoint path.


Current native SESSION admits non-distinct COUNT/SUM/AVG/MIN/MAX, ordered
FIRST/LAST and non-distinct ARRAY_AGG, including ordered arrays. ARRAY_AGG values
and ordering expressions must be direct input columns; an optional FILTER must
be a direct Boolean input column. The typed fixtures materialize their Boolean
member in the source rather than expanding an expression during collection.
Array ordering, element types and nulls follow DataFusion semantics.

Collection admission pages all retained rows before evaluation and bounds the
sum of decoded input memory by `window.partial_bytes`; the final aggregate must
also fit that limit. Collection and sorting scratch is admitted separately in
the configured execution memory pool, using checked multiplication of retained
input memory by 16 and the sum of each collection's value and ordering column
counts. This reservation may exceed `partial_bytes`; insufficient execution
pool capacity returns an admission error before collection evaluation. Oversized input returns
`native SESSION collection input exceeds configured value limit`; oversized
output returns `native SESSION output aggregate exceeds configured value limit`.
Limits are not increased and output is not truncated.

The typed collection/reused-key cases are prepared regressions, not current-source
runtime passes. Slow-output backpressure, source idleness/markers, sink delivery
failures and rescaling remain separate acceptance checks. Heavy capacity, actual
retained checkpoint measurement, RSS, protocol/resource/fault execution and soak
qualification remain on STR-32. No new runtime qualification is claimed here.

External application lifecycle parity remains unproved: minimum event timestamp
versus first arrival, gap-added end versus last event end, strict-greater closure
versus inclusive closure, native gap splitting versus named-ID timer reuse,
watermark-late dropping versus accepting older arrivals, and any maximum-duration
rule must be evaluated against application-owned fixtures. No oracle here changes
those policies or authorizes new deadline/clock/emission semantics.
