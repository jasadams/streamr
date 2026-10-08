# Native updating-aggregate retained-capacity qualification

`scripts/test-native-aggregate-capacity.py` prepares generic inputs and immutable
checkpoint/final JSONL oracles before running the real SQL planner and full
checkpoint capture. It has not been executed as part of this preparation.
The coordinating agent runs the built executable serially in the required
development container. Small fixture success is not a larger-than-RAM result.

The current native aggregate harness uses **78 MiB**, not the native window
harness's 50 MiB: executor16 + block cache8 + memtable4 + queued writes32 +
decoded16 + scan2. These existing settings remain unchanged, including aggregate
key512 bytes, value32 KiB, scan page128 KiB, write scope2 MiB and output512 KiB.
No test resource overrides are set. `--require-10x` checks both declared retained
floors against ten times78 MiB **before any fixture writes**.

| Scenario | Existing SQL | Checkpoint and retained payload premise |
|---|---|---|
| many | Indefinite COUNT(*) + MAX(payload) by opaque key, append-only input | Two independent payloads per key, followed by one empty-payload update for every key after checkpoint. At prefix199999 of300000 events every100000 keys exists; last key count1, all others2. Final count3 and exact MAX for every key require reading actual restored state. One complete current MAX value per key provides819200000 bytes, exceeding817889280 (10×78 MiB). Do not count both input payloads as retained history. |
| hot | CDC input, one opaque key, COUNT + indexed MIN(metric), MAX(metric), LAST_VALUE(payload ORDER BY ordinal) | Configurable `--rows`, default8, payload256 bytes. Inserts, then deletes minimum/maximum members and replaces the remaining last member. Checkpoint rows-minus-one is a proper input prefix. LAST_VALUE's indexed M/R stores actual args; an unused input payload or scalar COUNT alone is not a retained-byte premise. |

The **large hot8192-byte shape has a demonstrated source-level admission gap**:
`native_member_keys` embeds the entire encoded argument tuple in the reverse R
key; that exceeds the unchanged512-byte key cap. M values alone fitting32 KiB
does not make this query admissible. This is a valid configured-limit contract,
not evidence that the engine needs an architectural repair. An8192-byte small
fixture can separately capture the exact refusal; it is not capacity evidence.
Small64/192/256-byte values test the existing indexed path, with actual admission
required before relying on the premise. About3.2 million256-byte members would
be needed for10×78 MiB; root first uses small/intermediate runs to budget runtime
and disk before selecting that large scope. No key limit, SQL syntax, timer or
collection gate is changed. No ARRAY_AGG materialization is used. Passing scalar
indexed capacity would not solve the separate hot-profile top5 full-array
materialization limitation.

Run small cases first, using new directories:

```sh
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-many-small --scenario many --keys 8 --payload-bytes 64 --binary BINARY --backend memory --backend rocksdb --source-evidence BUILD_SOURCE_EVIDENCE
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-hot-small --scenario hot --rows 8 --payload-bytes 64 --binary BINARY --backend memory --backend rocksdb --source-evidence BUILD_SOURCE_EVIDENCE
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-hot-256-small --scenario hot --rows 8 --payload-bytes 256 --binary BINARY --backend memory --backend rocksdb --source-evidence BUILD_SOURCE_EVIDENCE
```

The names describe independent keys/members and UTF-8 payload byte lengths.
Prepare only by omitting `--binary`. Root checks free disk and active
runs before larger cases; input/oracle/captured payload text can occupy several
GiB, even though each child has a512 MiB whole-process RSS ceiling.

```sh
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-many-100000 --scenario many --keys 100000 --payload-bytes 8192 --require-10x --binary BINARY --source-evidence BUILD_SOURCE_EVIDENCE
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-hot-intermediate --scenario hot --rows 10000 --payload-bytes 256 --binary BINARY --source-evidence BUILD_SOURCE_EVIDENCE
```

The intermediate hot scope does not reach10× and must not be labeled capacity
qualified. A separate small configured-limit diagnostic is:

```sh
python3 scripts/test-native-aggregate-capacity.py /app/target/aggregate-hot-key-limit-negative --scenario hot --rows 8 --payload-bytes 8192 --binary BINARY --source-evidence BUILD_SOURCE_EVIDENCE
```

Expect nonzero capture and preserve its log. Do not combine that expected refusal
with successful capacity results.

Both checkpoint protocols run in serial by default; RocksDB is default backend.
Explicit backend/protocol flags limit a scope. Source batches32, ordinary flush
period3600 seconds, initial ready tick, checkpoint flush and EOF follow the
unchanged harness. Intermediate row counts can vary; every whole aggregate row
must equal one declared input-prefix value, and every CDC before image must match
the previous image exactly. Checkpoint epoch1/proper source prefix/committed output
prefix and all materialized keys are checked separately from final endpoints.
The many-key COUNT/MAX query rejects unchanged duplicate CDC updates and requires
count3/full MAX after the post-restore input touches every key. This assertion
does not change general composition CDC policy. The hot query checks exact
selected minimum/maximum/last values after retractions/replacement; it does not
read back every retained member payload.

Full payload contents are independently reproducible SHAKE-generated ASCII;
comparisons preserve integer versus boolean/float types, every output field,
and complete string contents. Oracles are written before execution and pinned
against modification. Streaming comparison retains only compact per-key counts
or hot stages, not all payloads or all historical snapshots. The Python parent
and its generated oracles are **outside** the measured SQL child RSS; verifier
memory is not measured or advertised as bounded by the child's pool sum.

Per-protocol inventory pins compiled sources, used runner/imported helper,
binary, rendered query, input, declared fixture, both oracles and required host
build evidence before/after capture. No git command is required in the container.
The host snapshot's `compiled_sources`, `workspace_build_files` (Cargo manifest
and lock), reviewed capture hooks, repository revision, compiled diff hash and
`sql_test_sha256` are validated against mounted bytes. The coordinator owns the
build/gate provenance; hashes alone are not an independent rebuild.
Unrelated docs/new scripts do not abort a running capture. Logs/child RSS and
successful exact comparison artifacts are preserved in new directories; runtime
failure does not create a successful comparison. Child deadlines kill/reap via
the existing foreground wait4 runner. No process-loss/Kafka/sink-commit/parity or
checkpoint-format replacement is claimed by SQL-test worker recreation.
