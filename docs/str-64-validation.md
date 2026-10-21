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

The independently reviewed scan-only patch and helper repair passed maintained formatting, affected locked all-target check and strict Clippy. All five focused diagnostic/compatibility checks, 26 batch8 core/correction reruns, and all twelve selected scan/fault SQL cases exited 0. Matching state/worker/SQL test executables and source hashes were preserved in the same queue reservation. The queue driver still exited 1: ten normal evidence summaries failed while reading the hyphenated configuration key, including one interleaved diagnostic JSON record. The original failures remain in `scan-validation-repair2/summary.json`; offline evidence reconstruction/audit is pending, and resource qualification is not yet claimed. The recovered queue receipt confirms completed exit1 and no active build. Diagnostics-off cost and diagnostics-on traversal/lifetime observations are separate fresh processes; neither source-derived counts nor output equality alone establishes native resource behavior.

## Remaining acceptance

Required full-ticket acceptance remains open for measured changed-scan resource/lifetime qualification, coherent multi-dependency completion/reconvergence, selector/consent and JSON/map/join composition. Direct paired corrections/deletes and accepted absence now pass the selected repair6 SQL matrix. Unsupported routes remain explicitly rejected; STR-64 is not complete. A narrow branch-completion proposal awaits the user's architectural decision; no new carrier is implemented. STR-60 owns implicit selector ordering; STR-65/66 own broader maintained/map prerequisites.

No OS-process-loss, sustained resource qualification or global Ready-manifest publication assertion is claimed. Existing whole-attempt failure and completed-checkpoint restoration are preserved. No application-specific engine behavior or checkpoint redesign is implemented.
