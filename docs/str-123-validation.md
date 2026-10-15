# STR-123 dirty aggregate flush traversal

Fresh fetched base `2368459dacd53f9e3aa5024fb5a4f31ba96f8dab`, branch
`jason/str-123-dirty-20261015`, workspace
`/home/jason/repos/streamr-worktrees/str-123-dirty-20261015`. Read current
STR-123, `AGENTS.md`, `.claude/team.md`, `.claude/build-test.md`, current
milestone ledger/work plan, build QA troubleshooting and implementation role.
STR-120 retains extrema/member ownership; STR-122 retains updating joins.
No signing or automatic elapsed-time rule applies.

## Contract and smallest repair

Preserved upstream Arroyo source at revision
`18c203dcdd5515ab36237b2f02721570d6f20eef` tracks changed groups and their prior
values in `updated_keys` before a flush evaluates CDC. The native fork uses D
markers to discover those groups on disk. Current native drain advances its
logical `after` key within one invocation, across committed chunks, and starts
every new invocation cold. This is necessary to discover newly dirtied earlier
keys. Its existing state/D commit precedes collector completion; the change
preserves that ordering, including cancellation/retry behavior.

Only dirty enumeration now asks the backend for one entry on its first read,
and two entries when an inclusive native `after` may appear in the chunk's stable
snapshot. Previously it fetched the whole configured page and returned only the
first live entry. All other prefix readers retain their configured cardinality.
The existing overlay merge and page continuation remain unchanged; configured
one-entry and byte-limited pages still progress normally. No limits, output
values, indexes, retained progress, checkpoint format, SQL, resource pools or
backend-specific execution path change.

## Scan reporting

Enumeration is required because dirty group identities are unknown; known-key
point reads cannot discover them. The scan retains namespace plus logical D
prefix. The native lower bound is the greatest of escaped namespace/start,
escaped namespace/D prefix and any encoded last-entry cursor. The native upper
bound is the exclusive prefix successor of namespace/D (namespace/E). Logical
`after` stays exclusive through the existing equality filter; native Seek remains
inclusive. Each dirty group is still discovered independently, with a final
exhaustion check and existing chunk boundaries. No cursor or snapshot crosses a
flush, error/cancellation or recovery. A new invocation discovers earlier keys.

Snapshot, scan reservation, iterator and decoded/write/output lifetimes remain
unchanged. Each chunk owns a stable snapshot through its existing commit; each
native iterator exists only for the admitted scan task; at most one page is
retained while filtering/merging overlay entries. The reduced cardinality is
bounded by the original configured maximum and requests the same maximum byte
budget. Generic `first_from` remains unchanged. Many arbitrary pending overlay
deletions can require more short pages; the production dirty caller advances
past its previous deletions and only equality needs room in its stable chunk.
No claim of improved generic prefix scanning follows.

## Finite baseline and measurement scope

Raw source inventories/copies, patches, commands/exits, immutable test executables,
image identity and logs are preserved outside Cargo caches at
`/home/jason/qa-evidence/str123-dirty-20261015/`. Purpose: focused correctness and
finite traversal diagnostics; actual profile: normal dev/test, matching
precompiled RocksDB O0 archive, immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Disk availability before build was150 GiB. Maintained scripts and the shared
queue serialize builds and dependent runs; binaries are copied before releasing
the same reservation.

Baseline `baseline/` built and ran successfully before production edits. The
opt-in real `drain_native_dirty` diagnostic seeds2,048 D markers, pins that
pre-deletion history, deletes them, then performs96 flush cycles of four sparse
markers alternating earlier/later keys. Compaction remains at its ordinary
setting. Missing G records deliberately exercise the established stale-marker
cleanup branch, isolating D enumeration from codec and output costs. Each cycle
checks all four marker removals after acknowledged commit; exactly384 markers
complete and zero CDC rows are emitted. This is not a SQL/output throughput or
capacity qualification. Exact real-group CDC checks are separate below.

Temporary opt-in PerfContext instrumentation records every actual D adapter scan:
issued/native child/memtable seeks, returned rows, internal/deleted/recent skips,
block reads/bytes and seek/next timing. `/proc/self/stat`, `status` and `io` are
captured at each completed32-cycle stage. Process counters include setup,
background and diagnostic work; scan counters do not cover point-read/write I/O.
Returned rows alone do not describe native traversal.

| Baseline completed cycles | Dirty completions | Native seeks | Fetched rows | Internal skips | Deleted skips | Stage milliseconds |
| --- | --- | --- | --- | --- | --- | --- |
|32|128|160|448|170289|163739|169.042|
|64|256|160|448|171296|163744|199.149|
|96|384|160|448|171296|163744|178.488|

Each row except completed cycles/markers is a stage delta. Baseline compilation
and pilot exited0; no failed baseline check was replaced. The first candidate inventory (`candidate-probe/`) completed every selected
independent scenario: build/pilot/store exited0; incremental inventory exited101
with71 passed, one failed and five ignored diagnostics. The only failure was the
new CDC fixture's use of a point-only scope for indexed retractions, which need
the established stable member scan. Its event scopes now use ordinary `begin`;
marker-only scopes remain point-only. Production code, assertions and budgets
were unchanged by this fixture repair. The complete original failure/source/pin
is retained. Repaired full inventory and alternating comparisons subsequently passed as
recorded below. Final probe-free gates subsequently passed below; actual OS recovery subsequently passed below.


Repaired retry job `9a5ffce8ff4d` was refused before its runner/build started:
"Insufficient disk reserve while an unqueued Cargo build is active." Its exact
queue log, command/exit1 and read-only process/disk/container inventory are in
`infrastructure-refusal-r2/`. Active unowned Trakkt wasm/UI and server builds were
preserved; no Streamr build container remained active, and150 GiB was free. No
reserve bypass, unrelated kill, cache deletion or production change was made.
This is infrastructure refusal, not a failed repaired test. Both unowned
Cargo builds then exited normally; supported retry `d0b4d29642a2` started
after the ordinary guard reclaimed2.99 GiB of regenerable shared cache
(150.5 GiB free). That retry subsequently passed the repaired inventory and alternating
comparisons below; final probe-free gates subsequently passed below; actual OS recovery subsequently passed below.

## Matched measurements and residual work

Repaired `candidate-probe-r2/` built/pinned and passed the explicit diagnostic,
four store tests and72 incremental tests (five opt-in diagnostics ignored), all
exit0. The store matrix includes memory/RocksDB and one-/three-entry configured
pages, overlay insertions/deletions, an absent committed prior-chunk `after`,
cold earlier reinsertion, rollback and cancellation while native workers are
occupied. Its assertions compare the dirty helper with ordinary ordered reads.

`pairs/receipt.json` preserves three alternating AB/BA/AB probe pairs and two
AB/BA probe-disabled pairs. Every one of the ten finite arms exited0 and
completed the same96 flushes/384 marker removals with zero CDC output. Identical
input keys, pinned history, clocks, native/profile image, limits and sink were
used in both arms. `summary.json` retains every stage, native counter and process
sample. The fixture-only scope repair and store regression additions do not alter
the measured diagnostic's input or runtime path.

| Native work per96 flushes, each of three matched diagnostic pairs | Baseline | Candidate |
| --- | --- | --- |
| Issued seeks |480|480|
| Returned rows |1344|768|
| Native memtable seeks |2760|1843|
| Internal keys skipped |512881|405654|
| Deleted keys skipped |491227|392782|
| Child seeks / recent skips / block reads / block bytes |0 / 0 / 0 / 0|0 / 0 / 0 / 0|

The cap removes discarded page work. It does **not** eliminate historical
traversal or cold prefix restarts:392,782 deleted-key skips remain in every
candidate arm. The workload stays in native memtables; zero scan block reads do
not establish disk-I/O improvement. It is neither larger-than-RAM qualification
nor a new cross-flush progress protocol.

| Alternating diagnostic pair | Baseline seconds | Candidate seconds | Baseline early / middle / late markers/s | Candidate early / middle / late markers/s |
| --- | --- | --- | --- | --- |
| AB0 |1.111297|1.275687|320 /375 /346|280 /291 /338|
| BA1 |1.456614|1.002820|335 /179 /357|353 /378 /424|
| AB2 |1.356729|1.045265|401 /260 /235|353 /353 /400|

Each rate counts128 **committed marker removals** across32 flushes, including
marker puts and post-commit point assertions. Source receipt and sink CDC
completion are separate; this diagnostic has no codec or CDC rows. Pair0 regresses
in elapsed time despite lower traversal counters. Probe-disabled finite arms
likewise completed all markers: baseline→candidate0.968170→0.666038 seconds
(AB), and0.828916→0.778146 seconds (BA). These arms use the same diagnostic-containing baseline/candidate binaries
with the environment-controlled native probe switched off; they are not the
final probe-free artifacts. They remain dev-profile stale-marker diagnostics,
with substantial timing variation across runs. No stable optimized
production-throughput gain is claimed.

Diagnostic-stage RSS ranges are baseline104,468–106,216 KiB and
candidate106,304–106,572 KiB; probe-disabled ranges are baseline105,704–106,152
and candidate106,396–107,324 KiB. No RSS improvement is claimed. User+system CPU
deltas from the first completed32 sample to completed96 are baseline62/60/70
ticks and candidate58/56/54 ticks in diagnostic pairs; probe-disabled54/54 versus
45/45 ticks (100 ticks/second). These omit setup and the first32 cycles. Native
scan block counters are all zero. Whole-process final read_bytes include
72,314,880 bytes in the first diagnostic baseline and184,320 in the last
candidate, zero in the other diagnostic arms; write_bytes range442,368–475,136.
Probe-disabled final read_bytes are zero and write_bytes range438,272–499,712.
Those cumulative OS counters include executable/library/setup/WAL/logging work
and cannot establish a scan I/O reduction.

The temporary state probe has now been restored exactly to base SHA256
`5dbe209223b0dbeb3bad7db76986aac1456eb6e184329df018319155396ee169`.
`source-freeze.json` proves both changed worker files are byte-identical before
and after probe removal. No final state source diff remains. Final probe-free gates/pin passed within the same reservation as all matched
comparisons, without a cache-eviction boundary.

## Exact behavior and remaining gates

The new real-group CDC regression runs both memory and RocksDB through append,
later-key update, newly dirty earlier-key update, unchanged suppression,
retraction, complete deletion and reinsertion. It asserts all ten CDC images in
order, including group, retract flag, exact count/ordered first/last and event
timestamp. This is separate from the zero-output scan diagnostic. Existing
focused tests independently cover NULL/filter/duplicate/order/retraction values,
minimum pair/write budgets and exact finite-TTL checkpoint/backend restore.

The slow collector regression now checks that state/D removal and before-value
commit precede its blocked collector await, preserves the full output reservation
until completion or cancellation, releases it afterward, and a cold retry emits
no duplicate already-committed output. The store matrix separately cancels a
blocked dirty scan before it can mutate anything and proves scan admission is
released. All these repaired focused checks passed in r2; actual OS worker-loss recovery subsequently passed below. Independent final
report/integration review and publication/CI remain coordinator gates.


## Final probe-free checks

Queue job `d0b4d29642a2` and its foreground runner exited0. The final immutable
worker test executable SHA256 is
`3e1180d20c971d933d48f91674004ee9313092f428648e739eb74715362b28ff`.
`candidate-final/receipt.json` preserves exact commands/exits/timing, full
worker/state/RPC source hashes, artifact identity and source copies; executable
pinning and every dependent test occurred before reservation release. Its two
changed worker files match the passing measured r2 source, and the state source
matches base exactly. No final state change requires a changed-state crate gate.

| Command | Exit / result |
| --- | --- |
| `scripts/cargo-dev check --locked -p arroyo-worker` |0|
| `scripts/preflight-clippy.sh -p arroyo-worker` |0, strict all targets|
| `scripts/cargo-dev fmt --all -- --check` |0|
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib --no-run --message-format=json` |0, executable pinned|
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib arrow::aggregate_store -- --test-threads=1` |0,4 passed|
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib arrow::incremental_aggregator -- --test-threads=1` |0,72 passed /5 ignored diagnostics|
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-worker --lib native_dirty_history_pilot -- --ignored --nocapture --test-threads=1` |0,1 passed|

No unrelated failure was repaired. Original fixture failure, refused preparation,
all successful arms and raw counters remain preserved. Actual OS worker-loss recovery subsequently passed on its maintained dev CLI
package below. All requested local execution checks are complete. Broader combined capacity/production/1.2M
qualification remains separate, and no stable production-speedup claim follows
from these finite diagnostics.


## Actual OS worker recovery and evidence preservation

Maintained `scripts/build-pin` dev/prebuilt packaging and its source-free runtime
harness both exited0 in the same reservation `35389ad87c92`. Exact receipts,
frozen source, package/source/native manifests, commands, logs, checkpoint proof,
worker identities and every golden oracle are under `recovery/`; the compact
result is `recovery/final-recovery-summary.json`. Its CLI executable SHA256 is
`a17519e3ea1bb4174925988b34e225b8eba53912c186a71d108f8ba0f31bd775`.
The unchanged worker source freeze and all current compiled inputs match the
package's frozen inputs. Image remains `bad108ab…`, profile dev, native mode
prebuilt. These final documentation updates do not change compiled inputs.

Both RocksDB batch1 checkpoint protocols passed actual worker SIGKILL/restart:

| Protocol | Old PID / start / generation | Fresh PID / start / generation | Result |
| --- | --- | --- | --- |
| controller |23 /67155513 /1|122 /67159174 /2|passed|
| worker |246 /67163715 /1|337 /67169715 /2|passed|

Each case verifies the checkpoint epoch2/source-prefix2 boundary, exact
aggregate CDC prefix2 rows and final5 rows, and all maintained calendar/session/
state-table prefix/final oracles. Their prefix counts are2/0/2 and final5/3/5,
respectively. The recovery fixture has five input rows/two aggregate keys and
aggregate creation/update behavior; it does not test aggregate suppression,
retraction or delete/reinsert. The focused native operator tests cover those
mutations separately. This is real source-free OS worker recovery, distinct from
the in-process fresh-backend tests and zero-output dirty-history diagnostic.

Both owned cleanup results are complete with empty errors. Nominal0.2-second
sampling observed owned RSS421,122,048 /420,724,736 bytes, below the unchanged
512 MiB limit; synchronous API/proof gaps are recorded. Those samples are not
continuous peak or cleanup RSS and do not establish capacity, dirty-history
throughput, memory-backend process recovery or full qualification.

Four recovery queue preparation refusals are retained in `recovery/queue-refusal`
JSON files through `queue-refusal-r4.json`, alongside the earlier implementation
retry refusal in `infrastructure-refusal-r2/`. Unowned builds were preserved;
no reserve bypass or unrelated cleanup was used. To recover reserve, the three
owned inactive measured/failed worker test executables were subsequently
losslessly archived as `baseline/worker-tests.zst`,
`candidate-probe/worker-tests.zst` and `candidate-probe-r2/worker-tests.zst`.
Before unlink, each archive stream decompressed to the exact original size and
receipt SHA256; mode/path/hash/archive hash and restoration commands are in
`compression-receipt.json`. Source copies and failed/successful logs remain.
The current final test pin stays uncompressed and unchanged. Archival freed
1,980,065,659 bytes; it did not discard evidence bytes.

Reference audits found no known STR123 validation/runtime references. The full
fuser/lsof exits/stderr and specific protected preexisting systemd/sd-pam/sshd or
zombie identities are retained in `compression-protected-process-audit.json`;
blanket visibility into those protected processes is not claimed. Initial
conservative audit refusals are retained too. Coordinator and independent
reviewer approved this lossless evidence preservation before archive/unlink.

Independent local source/evidence review is approved in
`review-local-approved.md`, including all final source hashes and local gates.
The final report/recovery evidence review, publication and current-head CI remain
with the coordinator. No local execution acceptance is deferred; broader
combined production/capacity qualification remains separate.
