# STR-64 native per-event aggregate → MERGE validation

STR-64 remains open. This is a reviewed and tested core increment, not full-ticket completion or publication. Current integrated base: `eb5127dba7ac90ec0bea44249b0ec0ca27db46c8` (rebased core `69e7581d66a43016c94b74e16fe077da7e429394`); historical append-core validation base: `8f937aa9b64e3dafd8af3c0c9bd7d733a208900e`; initial fetched base: `14353046e5085325b66563981be61c96ecfed7b7`. Guidance re-read after integration: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, current ticket and milestone ledger. Evidence root: `/home/jason/qa-evidence/str64-event-select-20261018/`.

## Implemented boundary

A private planner route selects per-event results for one keyed native non-distinct COUNT/MAX aggregate feeding MERGE, with one original source, singleton ownership and `SET updating_ttl = NULL`. Native scalar projections, source WHERE, static aggregate FILTER and downstream HAVING remain available. The direct correction route additionally admits validated complete Debezium operations through deterministic scalar/filter transport. Nested maintained dependencies, joins, distinct/ordered/non-COUNT/MAX aggregates, calendar aggregates and finite retention remain rejected. Serialized worker construction independently validates the supported configuration.

Existing aggregate kernels already update every input contribution; ordinary dirty-key emission can coalesce several same-key events. The new route evaluates those native accumulator states after each accepted row and emits immutable ordered append snapshots, including unchanged results, in bounded batches. COUNT prefixes `1,2,3` and MAX `10,20,20` retain trigger times `10,20,5`. Only the internal result timestamp is replaced with the original input time; selected user MAX values remain native aggregate results. No aggregate arithmetic is moved into MERGE or a UDF.

The current clock function is `EVENT_TIMESTAMP()`. Historical ticket references to removed `WATERMARK_TIMESTAMP()` do not add an alias. Clock lineage skips only the validated per-event aggregate node and still checks its input for one source and unmodified time. Ordinary aggregate output time remains rejected as an event clock. Named ordinary/event producers remain distinct; the mixed-mode planner identity regression is not a runtime composition claim.

The aggregate and MERGE retain separate state ownership. Existing MERGE reads its earlier staged target writes, batches bounded backend writes and captures source/old/new/action once for both downstream consumers. Awaited delivery and existing checkpoint alignment govern consistency. Failure abandons the whole attempt; fresh aggregate/target owners restore the same completed checkpoint before replay. No shared live transaction, new completion protocol, checkpoint format or admission mechanism is introduced. Main's existing generic chaining fanout repair is reused unchanged; this task adds only further optimizer controls.

Event evaluation uses existing native accumulator evaluation; fallback Batch accumulators are rejected. MAX evaluation uses existing indexed prefix scans, and per-operation emission changes their frequency. Correction/retraction and empty-group validation also perform existing member/index scans. These changed execution paths require measured traversal and lifetime qualification; that qualification is pending below. The opt-in diagnostic patch changes no iterator options or resource limits. Event-result chunks honor configured output row/byte, concatenation, write and overlay limits. Unit regressions verify cap2 with eight events, exact prefixes/clocks, structured error propagation and decoded-permit release.

## Historical append-core validation

Purpose: focused correctness and recovery. Profile: normal dev/test; matching precompiled RocksDB. Immutable dev image: `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`. Maintained `scripts/cargo-dev` and `scripts/preflight-clippy.sh` run inside one `scripts/rust-build` reservation per inventory, sharing the canonical target. Disk before integrated validation: 102 GiB free on `/home`, 5.5 GiB on `/tmp`. Native selector was verified after the Cargo.lock integration. Documented generated-RPC freshness is restored by touching proto inputs inside the same reservation; source bytes are unchanged.

| Integrated check | Result |
| --- | --- |
| Five affected crates, locked all-target check | PASS, exit 0 (repair7, 1m02s) |
| Maintained strict Clippy, same five crates | PASS, exit 0 (repair7, 1m05s) |
| Datastream optimizer | PASS, 4 tests |
| Planner MERGE / event-clock / state-table fusion | PASS, 31 / 20 / 20 tests |
| Worker aggregate / state-table filters | PASS, 83 / 30 tests; six existing ignored |
| RPC retention wire presence | PASS, 1 test |
| Final SQL fixture locked all-target check / strict Clippy | PASS, exits 0 / 0 (repair8, 54.24s / 45.89s) |
| Memory, source batch1 / batch8 | PASS, 5/5 each; exits 0 / 0 |
| RocksDB, source batch1 / batch8 | PASS, 5/5 each; exits 0 / 0 |
| Maintained formatting | PASS, repair7 and repair8; exit 0 |

`gates-repair7.sh`, `gates-repair7.tsv`, `gates-repair7-summary.json`, `source-repair7.json` and individual logs preserve commands, exits and hashes. Final SQL test-only repair uses `gates-repair8.sh`, `gates-repair8-summary.json` and its corresponding source/log/result receipts. Repair8 reservation: `7ab1a829668e`; source-repair8.json SHA256: `ffb9b680cb23a4b4b6be41b80cd69825da51ddd0007f0fb29ea35530822c5079`. Only smoke_str64.rs and the coordinator ledger change between the two receipts (`validation-source-reuse.json`); production and other selected suite source hashes are identical. All 20 explicit test executions pass; eight fault receipts record both blocked-output boundaries, surfaced failures, source ACK and abandoned attempts. Source is frozen throughout each inventory; suites use `--no-fail-fast`, and every selected backend/batch invocation completes before repairs.

The five real SQL tests use the planner, serialized worker construction and local Engine. Their independent oracles cover COUNT/MAX prefixes, original trigger time, composite-key isolation, MAX-only unchanged/no-action old=new, one captured mutation consumed twice, source WHERE/aggregate FILTER/HAVING, ordinary append MERGE and completed-CP1 fresh-owner recovery. Observed source batches prove real batch8 execution (`[8,2]` uninterrupted, `[8]` clean suffix), not a configured maximum.

The two fault tests block the real collector after native aggregate commit or target commit. They retain both sinks at prefix2, request real CP2, require the source snapshot ACK while the blocked owner cannot complete CP2, surface the injected TaskFailed, abort/drain the whole attempt, and restore fresh owners from CP1 for the exact full oracle. The fault-only source has eleven raw rows: ten expected captures plus a native WHERE-excluded tail at time19, keeping the source/writer alive after CP2. Fresh fault replay observes nine raw suffix rows (`[8,1]` or nine single-row batches) and still exactly ten total captures on both consumers. Source ACK does not prove barrier enqueue at the owner or complete controller Ready publication. SQL/input/graph/batch/capture/fault artifacts are preserved under `runtime-*-repair8/`.

To reproduce the explicit SQL matrix from this checkout, use a maintained dev image with a matching native archive (`STREAMR_DEV_IMAGE` selects it when needed) and serialize all four configurations in one shared reservation:

```bash
scripts/rust-build bash -c '
str64_failed=0
for str64_backend in memory rocksdb; do
    for str64_batch in 1 8; do
        scripts/cargo-dev --exec env \
            STREAMR_TEST_BACKEND="$str64_backend" \
            STREAMR_TEST_SOURCE_BATCH_ROWS="$str64_batch" \
            STREAMR_TEST_TYPED_SQL=1 STREAMR_TEST_NATIVE_AGGREGATES=1 \
            STREAMR_TEST_CHECKPOINT_MODE=controller \
            cargo test --locked -p arroyo-sql-testing --no-fail-fast --lib smoke_str64 \
            -- --skip smoke_str64_branches --skip maintained_correction_scan_qualification --ignored --nocapture --test-threads=1
        str64_exit=$?
        printf "%s batch %s: exit %s\n" "$str64_backend" "$str64_batch" "$str64_exit"
        if (( str64_exit != 0 )); then str64_failed=1; fi
    done
done
exit "$str64_failed"
'
```

## Preserved failures and controls

Old-base results remain historical after integration. Initial build attempts exposed helper error conversions, stale shared-target generated RPC output, and a non-Send context borrow; their failed logs remain. Default-image native preflight failed before Cargo, and only unstarted owned reservations were cancelled (receipts retained). No failed or unstarted check is counted as passed.

Repair4's 16 SQL tests failed before worker construction on the removed clock name. The current-function focused regression then failed at the ordinary aggregate clock guard; the scoped lineage repair preserves that SQL and all original-time oracles. Repair5's 16 runtime tests exposed a disconnected capture branch. Broad optimizer controls and the ordinary append two-sink baseline passed; the exact three-edge helper counterexample failed, proving a latent inherited edge-index bug. Current main already fixed it, so the local alternate implementation was discarded during integration. Extra controls remain, and main's implementation is unchanged.

Integrated repair7 records 12 SQL passes and eight failures: every backend/batch combination passed values/clean recovery/filter/ordinary append, while both fault tests timed out on source CP2 ACK. CP2 released the final-row control wait and allowed source EOF before the asynchronous writer acknowledged. The reviewed excluded tail repairs only fixture lifecycle and raw batch expectations; all original capture, failure, owner noncompletion and fresh-replay assertions remain. Those timeouts remain recorded failures. Repair8 subsequently passes all 20 executions, including every fault variant, without changing product code or the accepted-event oracle.

Independent source review approved the integrated implementation (`code-review-integration-main8f937aa9.md`) and the final fixture lifecycle repair (`code-review-source-lifecycle-fix.md`). Their selected current-source gate conditions now pass.

## Direct correction validation — repair6

The direct route preserves hidden operation markers through a proven physical batch-preserving path. One bounded native aggregate scope applies both images of a replacement before producing final snapshots for up to two affected grouping keys. A final empty group produces no USING row and does not implicitly delete its target. The singleton queue fast path preserves complete batch order; source metadata remains private. Volatile or trigger-dependent pre-aggregate expressions and overwritten trigger clocks are rejected before worker construction. Existing separate owners, queue limits and checkpoint recovery remain in use.

Ordinary MERGE predicates combining OR and IS DISTINCT FROM exposed an upstream parser precedence bug. Both parser and derive now pin reviewed fork commit `41e07aa42c01ba8e9e625311f5e15202e0d5bf58`, using the coupled upstream fix without unrelated lock changes. Parser correctness: 1,137 passed. Its strict lint still fails on 52 pre-existing diagnostics (baseline 55); the independently approved baseline-relative classification is limited evidence, not a strict lint pass. Streamr's strict Clippy remains unchanged and passes.

| Current repair6 check | Result |
| --- | --- |
| Locked metadata, affected all-target check, strict Clippy, formatting | PASS, exit 0 each |
| Full planner library | PASS, 274; three existing ignored |
| Aggregate worker tests | PASS, 92; five existing ignored |
| Core and direct correction SQL, memory/RocksDB × batch1/8 | PASS, 52/52 |
| Native single-aggregate branch control, same four environments | PASS, 4/4 |
| Actual two-branch joined query | FAIL, 4/4: direct-aggregate planner restriction |

The eight correction scenarios cover paired replacements, source-envelope marker reset across a full batch, deletes/key moves/absence/recreation, source WHERE, aggregate FILTER, no-action capture, completed checkpoint replay and both post-commit delivery faults. An independent audit reconstructed the literals for all 32 correction executions: 256 expected captures per sink, exact mirror equality, raw/image/marker receipts and fresh-owner suffix replay. Fault runs observe TaskFailed, drain the abandoned attempt, then restore CP1 into fresh workers. This is same-process fresh-worker recovery, not OS-process-kill evidence. Source CP2 ACK does not establish owner barrier enqueue or global Ready.

Source-bound evidence: `source-correction-repair6.json`, `gates-correction-repair6.sh`, `.tsv`, `-summary.json`, corresponding runtime directories and `correction-repair6-runtime-audit.md` under the evidence root. Overall inventory exit is 1 because the four required joined cases fail. All selected tests finished on one frozen source before further edits.

The independently reviewed scan-only patch and helper repair passed maintained formatting, affected locked all-target check and strict Clippy. All five focused diagnostic/compatibility checks, 26 batch8 core/correction reruns, and all twelve selected scan/fault SQL cases exited 0. Matching state/worker/SQL test executables and source hashes were preserved in the same queue reservation. The queue driver still exited 1: ten normal evidence summaries failed while reading the hyphenated configuration key, including one interleaved diagnostic JSON record. The original failures remain in `scan-validation-repair2/summary.json`; the subsequent independent offline reconstruction/resource audit passes all twelve finite cases and reconciles all256417 diagnostic markers (see `scan-validation-repair2-independent-resource-review.md`). The original driver exit1 remains a tooling failure, not a passing driver result. The recovered queue receipt confirms completed exit1 and no active build. Diagnostics-off cost and diagnostics-on traversal/lifetime observations are separate fresh processes; neither source-derived counts nor output equality alone establishes native resource behavior.

## Selector/consent and shared source planning — 2026-10-22

STR-60 dependency `bfcb14e125f42b78ba98bf29563bec6337293ad2` is privately integrated; it remains separately owned. Typed shared-selector admission and deferred view clock binding retain native aggregate kernels. A generic local CASE resolver repair preserves the enclosing CASE result type when the selected child is untyped NULL, matching pinned DataFusion behavior. The application SQL and literal oracles are unchanged.

`selector-repair5/` records all twenty selected gate commands passing: normal dev/test with the matching precompiled RocksDB archive, locked six-crate all-target check, strict Clippy and formatting; response codec14, full planner310 (three existing ignored), state-table31 (one existing ignored); all52 core/correction SQL and consent4/selector-CDC4 across memory/RocksDB and configured batches1/8. Independent `runtime-evidence-review.md` reconstructs both new literal oracles and verifies both sinks, including the final consent unsubscribe timestamp NULL clear. The new packet harness records configured batch maxima; this alone does not prove actual eight-row callbacks. Historical compile, strict-lint, fixture and runtime failures remain preserved.

`selector-root-qualification2/` subsequently records all twenty gate commands passing on its own frozen source: check/strict/fmt, full planner313 (three existing ignored), the same60 SQL executions and six retained-selector qualification commands. The three additional planner tests prove one shared source root before branch predicates, canonical physical source expressions, unioned source columns and one named scalar capture. This root preparation is test-only; no live branch mode is enabled. The retained-state packet has65 original operations, checkpoint prefix32 and fresh suffix33, with64 independently specified captures per sink. Four finite page/diagnostic configurations and two blocked-owner failure/replay configurations exited0. Independent `independent-resource-audit/report.md` passes all six finite cases: both sinks match all64 literal captures; actual raw/unrolled observers prove prefix `[8,8,8,8]` and fresh suffix `[8,8,8,8,1]`; fault receipts identify actual aggregate/target owners, require TaskFailed, drain the failed attempt and restore completed CP1. All7204 diagnostics reconcile, including two exactly reconstructed tracing interleaves; all scopes/snapshots/output admissions release in order, and six unique owned container receipts prove cleanup. The additional timestamp comparator unit passes explicitly; it was not selected by the ignored integration filter.

The direct selector route reuses STR-60's existing bounded native winner-prefix reads. Winner ordering requires ordered enumeration within a known group/generation/aggregate prefix; a point read cannot identify the surviving winner after deletion. No new cache, admission pool or checkpoint protocol is introduced. The finite qualification preserves actual configuration, source batches, native prefix/page diagnostics, process CPU/I/O/RSS samples, serialized operator configuration and snapshot/output lifetimes. Actual serialized physical plans independently identify COUNT0/FIRST1/LAST2/injectedMAX3. Normal logical requests are128 selector winners +64 MAX +6 empty-group probes; native page1 records270 issued seeks,1742 internal-key skips and1212 deleted-key skips versus208 seeks/current64 (diagnostic cells include other namespaces). FIRST itself has65 seeks/63 deleted skips; LAST96/528. Each scan starts with Seek at its encoded prefix/cursor, ends at the prefix successor, obeys recorded page caps and advances its snapshot cursor; there is no iterate_lower_bound option. Page1 is not an internal-work bound. Snapshot/working-state release precedes public collector awaits; output ownership lasts through delivery/error. The report preserves exact limits and process CPU/I/O/RSS: the six finite cells take659–1290ms for whole harness phases, with sampled final RSS181176–186200kB. These are diagnostic observations, not isolated throughput, a whole-process budget proof or an improvement claim.

## Response transport and scalar/input components — 2026-10-22

`response-transport3/` records all24 selected gate commands passing on frozen source: maintained formatting, seven-crate locked all-target check and strict Clippy; types7, operator23, worker codec15/transport12/network9/pair5/scalar9; all60 existing SQL checks across memory/RocksDB and configured batches1/8. Test filters overlap, so these counts are not a unique-test sum. Executable copies and SHA256/source/image receipts are preserved inside the same shared queue reservation. The prior two strict-lint failures remain retained, with executable inventories explicitly blocked/not run.

Registered local queues charge empty responses at least one existing queue row; actual chained callbacks and awaited fanout preserve whole responses. Native IPC validation, original-output admission and checked decoded backing proofs bound payloads without a new admission pool. Canonical size preflight precedes encoding when ownership is reused; alias/subset/slice rebinding is checked and may compact a bounded payload. Temporary writer/frame/copy allocations are distinct from exact retained-size or whole-process RSS claims. Actual TCP tests exercise empty/key-only/SQL-NULL payloads and reject stale event/control identities before variable payload reads. Private response edges bind current execution generation and source/destination WorkerIds; ordinary Data/Signal framing is preserved.

The real indexed operator loop passes waiting-input, watermark/Idle skew, barrier alignment, restored Commit, EOF/terminal and chain-placement tests. Scalar tests use the existing physical executor, emit one empty response after WHERE filters all rows, preserve touched keys/context, retain native error classification and release obsolete input before blocked public delivery. These are component tests, not a whole Engine response graph or remote checkpoint proof. This increment changes no RocksDB scan.

## Native response callbacks — 2026-10-22

When explicitly configured, the source generator assigns one opaque context after native source admission and before branch predicates. Raw events remain one row; complete CDC replacements retain both images. Root delivery awaits fanout before the next original event. Native aggregates reuse existing retract/add and delta helpers; empty and unchanged branches each return one response, with SQL NULL values distinct from absence. The native JOIN waits for both responses, applies their existing changes, then resolves the final accepted relation. Both input queues remain parked through awaited public delivery; failure abandons the attempt for completed-checkpoint restore.

`response-native1/` retains two test-helper compile errors with all executable tests blocked. After the reviewed helper repairs, `response-native2/` records maintained formatting, locked affected all-target check and strict Clippy passing. Planner313, aggregate103, source16 and checkpoint19 pass (existing ignores and nested subprocess result overlap retained). All60 direct/core/consent/selector SQL scenarios pass on memory/RocksDB and configured batches1/8. The JOIN suite records14 pass/1 failure/1 ignore: its memory resource-counter lookup expected a scan-page gauge that was never initialized. The reviewed test-only correction explicitly acquires/releases the existing gauge before the blocked-delivery assertions. This setup is not RocksDB page-lifetime evidence. `response-native3/` passes all six affected gates: formatting, locked metadata, locked worker all-target check, strict Clippy, full JOIN15 pass/1 existing ignore, and final formatting check. Unchanged native2 suites and SQL retain their source-bound passes; no production code changed in this metric repair.

Response-mode aggregates evaluate native final values for each original operation's known affected keys rather than waiting for a coalesced dirty flush. Existing MAX member/winner and empty-membership reads can therefore run more often. No dirty-key scan or new index is added; actual backend-work/lifetime qualification remains pending.

This JOIN callback adds final reads of native retained relations for at most two affected full keys. Native rows are indexed under full join key plus row identity, so a known join key alone cannot select its current row or validate uniqueness with a point read. Each key uses the existing R-prefix visitor on both sides, capped at two returned rows per request; a second row fails uniqueness. There are at most four logical prefix requests per event and at most four returned rows for valid unique relations. This does not bound native iterator/tombstone work. The iterator seeks to the encoded prefix or advancing cursor and uses the prefix successor as its upper bound. Existing native per-row change probes remain separate. Final scopes/snapshots and physical pair passers are released before public delivery; the existing output permit lasts through delivery/error/cancellation. Actual memory callback tests cover those lifetime assertions; finite RocksDB traversal, CPU/I/O/RSS and blocked-delivery evidence remain pending before full acceptance. No index, cache, admission pool or recovery protocol is added.

These are native callback and direct-route regression results. Planner eligibility and Engine wiring are not enabled at this increment; joined Engine SQL, local lifecycle/recovery and finite changed-scan qualification remain required. The initial private response graph will require all response-edge endpoint assignments on one worker, validated before task startup, because existing assignment messages do not carry authoritative remote producer limits. Separate native owners and queues are preserved. No distributed response-graph execution pass is claimed.

## Remaining acceptance

The user approved exactly one context-bearing response per branch per original event, including empty payload, without separate completion Signals. Internal NULL SQL values remain distinct from absent rows and unchanged native deltas. Registered response transport, checked scalar callbacks and indexed JOIN waiting components are implemented and tested. No normal Engine job enables the route yet; source-root/native aggregate/native JOIN callbacks now pass their selected units; planner eligibility and actual Engine descriptor wiring remain to integrate and validate. Shared source-root planning remains test-only at this increment. No joined-prefix, remote fresh-attempt recovery or full-ticket completion is claimed.

Direct core/corrections and native selector/consent SQL now pass their selected matrices. Required multi-dependency reconvergence, empty/unchanged/skew behavior, branch scan/resource qualification and branch recovery remain open. STR-65/66 retain ownership of broader maintained/map prerequisites. No OS-process-loss, sustained resource, larger-than-RAM, whole-graph RSS or global Ready-manifest assertion is claimed. Existing whole-attempt failure and completed-checkpoint restoration are preserved. No application-specific engine behavior or checkpoint redesign is implemented.
