# STR-103/126 integrated MVP qualification

## Outcome and controlling scope

The integrated combined capacity/resource/process-loss qualification passed independent
raw-evidence review. STR-103 is Done. Fourteen additional current-source SQL recovery
cases also passed independent review. STR-126 delivery is awaiting current-head CI;
this report does not claim merge, release, application parity or cutover.

On 2026-10-17 the user accepted performance as sufficient for Arcstream MVP and
explicitly deferred remaining exhaustive per-path performance-only measurements.
Correctness, configured resource bounds and recovery remained required. The user
also identified significant host load: all rates are diagnostic observations, with
no Flink-parity, throughput-improvement or regression acceptance claim.

## Source and reproducible evidence

- Fetched main/base: `68539f93ecc2c7df01bb22f6b6f98eb693fcaaaf`.
- Production candidate: `e2ff41e7c991b3e0f0c68bd7d2f874631a00501b`, tree-identical
  to accepted STR-140 `c91787be`; rejected STR-141/142 are absent.
- Production binary SHA-256: `0e6078d2fe6f2a2c4279b4de3d7a8d6b5486b369b47a23a5c26dcd2bee363a36`.
- Production manifest SHA-256: `229d2d97e4c29bc6a4021d09a1f970388a5fdb043ea65598b809abf9783baa0b`.
- SQL-test source: `0e63174498ef3adb9848a669c4ea12e3c21110f3` adds only the
  final native-cleanup wait described below. SQL binary SHA-256:
  `b2e7d0df968fbe69babbfa0d13614d57e017b9bc211c5657a9cb476621750cc8`.
- Immutable Bookworm image: `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
  Maintained `scripts/build-pin`/`scripts/cargo-dev`/`scripts/rust-build` used
  the shared target and queue. Production qualification explicitly used release
  for optimized full-run timing; focused SQL correctness used normal dev/test.
  Matching prebuilt RocksDB release/host/dev archive routes were independently
  verified; no native-source override or extra target was introduced.

Evidence root: `/home/jason/qa-evidence/str126-qualification-20261017/`.
Exact commands, fixture/config/helper hashes and native receipts are preserved in
`runner-retry1/{preparation.md,frozen-controls.json,launch.py,run-qualification.sh}`,
`release-pin/`, `extra-correctness-retry1/{commands.json,sql-pin.json,source-manifest.json}`
and `extra-runtime-retry1/`. The pilot and large entrypoint runs under a real
canonical reservation; it is not an unlocked command to copy into a shell.
The [delivery receipt](str-126-delivery-receipt.json) binds final evidence hashes.

## Combined acceptance

The two-key/six-event pilot passed independent full-output/checkpoint/recovery
review before the large launch. Its observed peak RSS was 232,673,280 bytes.
The large graph retained four owners: scalar aggregate, calendar aggregate,
SESSION and native typed state table. It used 400,000 keys, 1,200,000 events,
2,048-byte opaque payloads, parallelism 1, blocking limit 2 and four native DBs.
The assigned backend pool stayed 62 MiB; summed owned-process RSS stayed capped
at 768 MiB. Queue/page/key/value/output/snapshot/disk-reserve settings are recorded
in `evidence/result.json` and unchanged from the accepted qualification limits.
Only the large generator moved from rate 1,000/batch 1 to the already-tested
rate 1e9/batch 8,192; logical event time remained 1,000 ns per counter. The pilot
retained slow generation to obtain a proper checkpoint prefix.

| Acceptance | Actual result |
| --- | --- |
| Retained typed keys at selected checkpoint | 400,000 |
| Actual retained payload, inspected from typed checkpoint values | 819,200,000 bytes |
| Actual encoded typed values | 1,256,000,000 bytes |
| Assigned backend pool / tenfold threshold | 65,011,712 / 650,117,120 bytes |
| Selected checkpoint / source counter | Epoch 2 / 417,792; exact start clock verified |
| Fault and recovery | Owned worker SIGKILL, worker 100/generation 1 to fresh worker 101/generation 2; all 14 readiness callbacks |
| Committed prefixes and replay | Byte-identical physical prefixes, exact CDC chains and nonempty fresh suffixes |
| Final scalar / calendar / typed-table output | 1,200,000 rows each, all values verified |
| Final SESSION output | 400,000 rows, all keys/counts/sums/start/end values verified |
| Observed peak owned RSS / cap | 350,818,304 bytes (334.566 MiB) / 768 MiB |
| Continuous RSS monitor | 22,382 samples; max gap 0.153970 s; zero errors; sampler joined |
| Runtime and cleanup | Engine Finished; controller/worker/client/container absent; mapping/progress threads joined |

The 100 ms monitor retains count/max/gaps and the peak PID/start-time vector,
not every raw 100 ms vector. Separate one-second raw process/mapping/progress
histories preserve zero-output intervals, recovery rewinds and checkpoint states.
Python oracle/reviewer memory is outside the owned engine envelope, explicitly
recorded rather than included in or concealed by the engine RSS number.

Independent review reparsed 920 selected-checkpoint artifacts, four owner identities,
schemas/namespaces/checksums, all 400k typed values, committed prefixes, full final
oracles and native source-clock/generation/readiness logs. See
`large-final-review.json`, `independent-large-audit.json` and the actual
`runner-retry1/attempts/large/{evidence,launch-result.json}` receipts.

Engine startup-to-observed-Finished was 1,908.823 s; runner plus final oracle and
cleanup was 2,286.214 s; host launch total was 2,317.503 s. Selected checkpoint 2
recorded 96.972 s and 128,809,336 artifact file bytes. Final case files summed
8,396,864,820 logical bytes; checkpoint files 852,241,529; live state 282,563,300;
outputs 7,261,733,350. These are file-byte sums, not unique physical device use.
Fault-to-first-observed replacement Running was 192.019 s, including polling and
scheduler/audit delay, not isolated RocksDB restore time. The diagnostic whole-run
rate was 628.660 logical fixture events/s including fault/recovery/EOF work; it is
not comparable to a ten-minute partial performance window. All timestamps and
checkpoint/rate/CPU/I/O observations remain available.

## Remaining native SQL paths and reused evidence

| Path | Current correctness/resource/recovery coverage |
| --- | --- |
| Updating ARRAY_AGG, NULLs and duplicate values | Two fresh memory/RocksDB controller/batch-1 captures, exact initial/prefix/recovered CDC and whole-process exit 0 |
| INNER/LEFT updating joins | Four fresh memory/RocksDB exact initial/prefix/recovered CDC cases |
| TUMBLE/HOP scalar and collection output | Eight fresh memory/RocksDB exact initial and recovered cases |
| Aggregate TTL, cleanup, membership, retractions and slow-output/cancellation | Accepted STR-140 source-bound 80 aggregate, 20 codec and 4 store tests; current production tree is identical |
| SESSION state/lifecycle and fallback | STR-138 source-bound accumulator/store/native gates retained; full current graph now verifies final scalar SESSION closure and actual process recovery |
| Public typed-table pagination | Existing STR-125 API evidence retained with explicit unchanged API/source closure; no invented production SQL caller. STR-139's state resource/recovery tests cover its changed telemetry path |

The 14 small SQL cases use fresh engine/task recovery, not OS-process-loss proof;
the combined production run supplies the actual SIGKILL evidence. Test binary,
profile, native linkage, all process exits and cleanup were independently audited
in `extra-final-review.json`. No new or changed RocksDB scan was introduced by
the QA repair. Existing scan purposes, native bounds, cursor/invalidation and
snapshot lifetimes remain documented in STR-109/111–125 and the STR-114 inventory.

Remaining exhaustive optimized-profile scan measurements are **DEFERRED
PERFORMANCE-ONLY**, not passed: SESSION deadline/global peek/metadata/history;
aggregate TTL/cleanup/duplicates/extrema/collections/last-member/dirty output;
calendar evaluation/pruning; window expiry/group/panes; join fanout; typed-table
API pagination; shared cursor traversal; checkpoint export/restore preflight and
SST-metadata sampling. Previous failed, regressed and unmeasured results remain
visible in the preserved earlier STR-126 report/worktree and per-ticket reports.
STR-120 cancellation is not an efficiency waiver. Broader qualification remains
STR-27; application acceptance/cutover remains ARC-17.

## Failures preserved and the only new repair

The first pilot attempt failed before engine startup on an inherited 172,800-second
assertion. A reviewed isolated caller repair enforces pilot 600 s / large 5,400 s;
actual-image dry-runs passed before retry. Original controls and failed evidence
remain in `runner/` and `runner-attempt1-preserved/`; no engine result was discarded.

The first extra SQL attempt passed the inner RocksDB array capture but aborted
at process exit with `pthread lock: Invalid argument`. The core shows RocksDB
DB destruction locking `mutex_locked_files` while another thread is in process
exit/global C++ destruction. The final restored capture omitted the existing
native-cleanup barrier used by earlier phases. The two-line harness repair drops
the restored engine and awaits that existing barrier before returning. No engine
contract, SQL, cache/resource policy or checkpoint protocol changed. Both independent
source reviews approved; fmt/check/strict Clippy and all 14 whole-process retry
cases passed. Original failure/core/reviews are preserved in `extra-correctness/`
and `teardown-investigation/`.

The conservative production manifest includes 598 source paths, including SQL-test
files. Exactly `crates/arroyo-sql-testing/src/smoke_tests.rs` differs after the QA
repair; the actual CLI artifact/dependency closure contains no SQL-testing crate.
The full production result remains attached to its actual e2ff41e7 pin, not renamed
to the amended source. The explicit closure proof is
`teardown-investigation/cli-unaffected-source-closure.json`; a repeat large campaign
would not test changed production code. The original 512 MiB failure and interrupted
768 MiB campaigns remain historical, without being relabelled as passes.
