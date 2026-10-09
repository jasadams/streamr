# Native state-table fixture parity

These five caller-defined fixtures use native state tables, named MERGE,
RETURNING and same-event LEFT lookups. `reference.json` inventories each native
fixture input and its independently declared ordered value oracle, including
content hashes. These expectations are not outputs captured from the query. Each checkpoint oracle
is an explicitly declared prefix of that full stream, with filtered events
accounted for. No stored state or checkpoint is rewritten by preparing these assets.

| Fixture | Input / output rows | Checkpoint source / output rows |
| --- | ---: | ---: |
| operations | 161 / 161 | 80 / 80 |
| shared_ctes | 161 / 161 | 80 / 80 |
| sequential_ctes | 6 / 5 | 3 / 2 |
| computed_cte | 6 / 5 | 3 / 2 |
| qualified_filter | 6 / 5 | 3 / 2 |

`manifest-batch1.json` and `manifest-batch8.json` use the existing bundle-runner
format. Both require ordered comparison, 120 seconds per SQL child and a 512 MiB
SQL-child RSS ceiling. Each package selects the existing `typed-sql` route;
the runner supplies a 16 MiB execution pool and the existing typed-state test
configuration. There is no queue-size override or pool increase. Source/sink
fields remain nullable. Singleton ownership is required; these rows do not
establish order across multiple owners.

The operations fixture covers null keys, null payloads, nullable predicates,
insert-if-absent results, conditional write/delete Booleans and a final actual
lookup. A null write value returns null without replacing stored data. The
shared fixture checks dependent reads and CONCAT expressions. The other three
retain original filters, field order, computed keys and dependent tables.
The operations stream does not independently cover an absent-key conditional
update without a preceding seed. The sequential fixture does not independently
read back every retained table value. Preserve these limits of the reference
streams; passing them is not exhaustive state-table semantics coverage.

## Reproduce with the existing bundle runner

Use the existing Debian development container and serialized root-owned test
queue. Select a newly qualified SQL-testing ELF and matching completed build
provenance. `package` records the supplied provenance; it does not verify that
all five build gates passed or that source bytes match it. Verify those first.
These commands do not build anything. Paths below are container-visible,
absolute paths within the existing `/app` mount. Set the variables explicitly:

```bash
DEV_CONTAINER=YOUR_EXISTING_DEBIAN_CONTAINER
SQL_BINARY=/app/target/milestone2-runtime/debug/deps/YOUR_QUALIFIED_SQL_TEST_ELF
BUILD_PROVENANCE=/app/target/YOUR_COMPLETED_BUILD_PROOF.json
FIXTURES=/app/scripts/fixtures/native-state-table-parity
FRESH=/app/target/YOUR_NEW_NATIVE_PARITY_CASE
CASE=operations
BATCH=1
BACKEND=memory
PROTOCOL=controller
```

Run one selection at a time in the existing foreground ownership/supervision
path. `FRESH` must be absent. The first command creates only this case's new
artifacts. The clean environment prevents inherited `ARROYO__` limits/queue
settings or test flags from changing the fixture. The owned TMPDIR below holds
ordinary temporary files; it does not relocate live RocksDB state. The existing
test configuration uses `/tmp/streamr-sql-live` inside the container. Independently
check filesystem free space and user-quota headroom there, as well as under the
results/checkpoint mount. Preserve active state and failed-run evidence; these
commands do not delete existing scratch or checkpoint files.

```bash
podman exec -w /app "$DEV_CONTAINER" sh -eu -c '
  test ! -e "$1"
  mkdir -p "$1/home" "$1/runtime-tmp"
' sh "$FRESH"

podman exec -w /app "$DEV_CONTAINER" env -i \
  PATH=/usr/local/bin:/usr/bin:/bin HOME="$FRESH/home" TMPDIR="$FRESH/runtime-tmp" \
  python3 /app/scripts/sql-capture-bundle.py package \
  --binary "$SQL_BINARY" --provenance "$BUILD_PROVENANCE" \
  --fixture "$FIXTURES/$CASE/manifest-batch$BATCH.json" \
  --route typed-sql --directory "$FRESH/bundle"

podman exec -w /app "$DEV_CONTAINER" env -i \
  PATH=/usr/local/bin:/usr/bin:/bin HOME="$FRESH/home" TMPDIR="$FRESH/runtime-tmp" \
  python3 "$FRESH/bundle/run.py" run --results "$FRESH/results" \
  --backend "$BACKEND" --protocol "$PROTOCOL"
```

Repeat serially over all five cases, batches `1` and `8`, backends `memory` and
`rocksdb`, and protocols `controller` and `leader`: 40 distinct cases, each with
a fresh path. Stop on the first failure and preserve its logs and artifacts.
Never copy a pending/running build ELF. Each ordinary package copies its ELF and
resolved libraries; check free space before packaging and do not blindly retain
40 duplicate ELFs. The separately reviewed qualification launcher deduplicates
its own immutable copied binaries; that optimization is not an option of this
runner. Do not chmod or relabel shared hardlinks. These repo-mounted reproduction
commands do not qualify physical source-free isolation.

The runner renders only `{{INPUT}}` and `{{OUTPUT}}` paths, clears inherited
STREAMR_TEST/STREAMR_CAPTURE flags and explicitly selects backend, protocol,
source batch, typed route, execution bytes and epoch 1. It retains checkpoints
under `results/<backend>-<protocol>/checkpoints`. Initial and recovered complete
rows must equal the entire golden in order, including names, JSON types, nulls,
Booleans and every value. The recovered prefix must equal the checkpoint golden.
JSON object member order is immaterial; row and nested array order are retained.

## Reproduction versus qualification

The existing bundle command validates the reported committed-row count and
complete initial/recovered/prefix outputs. It does not independently validate
all epoch/source-prefix/protocol/path/native-owner log witnesses, and it uses
the existing simple child supervisor. An interrupted Python runner is not by
itself proof that its SQL child was reaped. Execute through the reviewed owning
foreground supervisor and retain its cleanup evidence. The independent 40-case
qualification wave additionally checks actual configuration, exactly one native
fused owner, selected checkpoint metadata identity/path,
source prefix, epoch, retained object hashes, strict JSON parsing and full
source/build/binary/helper stability. These assets add no new harness API and
do not silently substitute standalone reproduction for that stronger evidence.

Only label an exact source/build revision qualified after its full matrix has
completed. Original SQL comments retaining the proposal status are preserved
byte-for-byte as preparation history; the completed qualification below applies
only to its recorded source/build and finite fixture scope.
These fixtures restore native checkpoints in the SQL-test process. They do not
establish production process-loss recovery, capacity or milestone completion.
The independent inputs and expected values are retained in each native fixture.

## Recorded v14b qualification

All forty captures passed on frozen source inventory
`d16b92b6ec3b44c741bd91bd5acc4762baaea594f94c6a3e1f372ee6b0040ca3`,
build evidence
`8791ffe6eb08f851463ab792095ae898fbe10f71dfd8a88b79daa3389f4f79fa`,
and SQL-testing ELF
`7e1fb8034285ead7e0bae311817fc0426ea095bfb6fd198dd122b2420387992b`.
The completed five Bookworm gates included 706 library tests across 25 suites
(six ignored). The preceding v14 test-compilation failure is preserved in the
milestone validation record; it is not a passing generation.

The actual matrix covers every row of the table above across both source batch
targets, both live backends and both checkpoint protocols. Complete initial and
recovered rows match the independent original goldens in order, including JSON
field sets/types, NULLs and Booleans. All declared checkpoint prefixes match.
Actual logs report epoch 1, the declared backend/protocol, one singleton native
fused owner. Job/lineage checkpoint paths and all 436 retained
object hashes were checked. The 16 MiB execution pool and 120-second/512 MiB
SQL-child limits are unchanged; no queue override is present. Child peak RSS
ranges from 182,763,520 to 191,946,752 bytes. Every child cleanup completed, with
final source/build/ELF/helper/fixture provenance stable.

Evidence root: `target/native-str43-parity-admission-v14b/`. Terminal result SHA:
`082da414721cd3ef83e01efcaabdb19d3c878e4fddae2ab7f990559a47ee5f69`;
results SHA:
`2d8d00989be791392265871b44bbc4e7380f74677f166fbcf4661e6b2d6cdc11`;
provenance SHA:
`9311ee954c8e06d64a46cfdfb5ddc9b51c83c240b730ae96f4e9d2ecd5604b66`.
The historical v13 partial pass and CONCAT-planning failure remain preserved.
The delivered fixture inputs/goldens remain byte-identical to their recorded
original references. Qualification came from the reviewed forty-case launcher;
the reproduction commands alone do not supply its stronger ownership/witness
checks. This was repo-mounted SQL-test fresh-worker native recovery, not physical
source-free isolation or production process loss. No independent raw-IPC typed
descriptor or persisted sink-offset decode is claimed by this matrix. The
reference-stream coverage limits above, full external application parity,
capacity and live/fault gates remain open.
