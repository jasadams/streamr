# STR-64 native per-event aggregate → MERGE increment

STR-64 remains open. This is a reviewed and tested core increment, not full-ticket completion or publication. Integrated base: `8f937aa9b64e3dafd8af3c0c9bd7d733a208900e`; initial fetched base: `14353046e5085325b66563981be61c96ecfed7b7`. Guidance re-read after integration: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`, current ticket and milestone ledger. Evidence root: `/home/jason/qa-evidence/str64-event-select-20261018/`.

## Implemented boundary

A private planner route selects per-event results for one keyed native non-distinct COUNT/MAX aggregate feeding MERGE, with one append source, singleton ownership and `SET updating_ttl = NULL`. Native scalar projections, source WHERE, static aggregate FILTER and downstream HAVING remain available. Updating inputs, nested maintained dependencies, joins, distinct/ordered/non-COUNT/MAX aggregates, calendar aggregates and finite retention are rejected for this increment. Serialized worker construction independently validates the supported configuration.

Existing aggregate kernels already update every input contribution; ordinary dirty-key emission can coalesce several same-key events. The new route evaluates those native accumulator states after each accepted row and emits immutable ordered append snapshots, including unchanged results, in bounded batches. COUNT prefixes `1,2,3` and MAX `10,20,20` retain trigger times `10,20,5`. Only the internal result timestamp is replaced with the original input time; selected user MAX values remain native aggregate results. No aggregate arithmetic is moved into MERGE or a UDF.

The current clock function is `EVENT_TIMESTAMP()`. Historical ticket references to removed `WATERMARK_TIMESTAMP()` do not add an alias. Clock lineage skips only the validated per-event aggregate node and still checks its input for one source and unmodified time. Ordinary aggregate output time remains rejected as an event clock. Named ordinary/event producers remain distinct; the mixed-mode planner identity regression is not a runtime composition claim.

The aggregate and MERGE retain separate state ownership. Existing MERGE reads its earlier staged target writes, batches bounded backend writes and captures source/old/new/action once for both downstream consumers. Awaited delivery and existing checkpoint alignment govern consistency. Failure abandons the whole attempt; fresh aggregate/target owners restore the same completed checkpoint before replay. No shared live transaction, new completion protocol, checkpoint format or admission mechanism is introduced. Main's existing generic chaining fanout repair is reused unchanged; this task adds only further optimizer controls.

No new production RocksDB scan is introduced. Event evaluation uses known-key point reads and existing native accumulator evaluation; fallback Batch accumulators are rejected. Existing ordinary dirty/TTL/calendar/member scans are unchanged. Event-result chunks honor configured output row/byte, concatenation, write and overlay limits. Unit regressions verify cap2 with eight events, exact prefixes/clocks, structured error propagation and decoded-permit release.

## Current-source validation

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
            -- --ignored --nocapture --test-threads=1
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

## Remaining acceptance

Required full-ticket acceptance remains open for paired corrections/deletes and accepted absence, coherent multi-dependency completion/reconvergence, selector/consent and JSON/map/join composition. Unsupported routes remain honestly rejected; this increment does not close STR-64.

This increment does not claim stronger same-attempt joint rollback, OS-process-loss or sustained resource qualification, Ready-manifest publication assertions, partial-fanout fault qualification or additional leader-path qualification. These are distinct from the selected fail-whole-attempt/completed-checkpoint recovery contract; they are not silently added as prerequisites to this core increment. No application-specific engine behavior or checkpoint redesign is implemented.
