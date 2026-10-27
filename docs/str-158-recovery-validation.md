# STR-158 Kafka checkpoint recovery

## Contract and current result

The controller restores the newest `ready` or `committing` checkpoint. An incomplete `inprogress` checkpoint is excluded: native state/source positions restore from the previous completed checkpoint and its source interval replays. A durable `committing` checkpoint instead restores that checkpoint and finishes its pending Kafka commit. The strict exact-marker guard refuses to claim success if the transaction cannot commit and its exact marker is absent.

This matches the retained [upstream Arroyo scheduler](https://github.com/ArroyoSystems/arroyo/blob/5f6e70eb2c8b4fbc808ab60b109c3d9d73e1b9e8/crates/arroyo-controller/src/states/scheduling.rs) checkpoint/transaction boundary. A durable pending Kafka transaction that expires cannot be reconstructed from Streamr's compact transaction metadata. Increasing the configured transaction timeout is prevention for future transactions, not recovery of expired payloads. The original production cause remains unproven; the isolated expiry cases establish a sufficient mechanism.

No ordinary-recovery product defect has been demonstrated. The current increment adds source-bound diagnostic and recovery qualification to the maintained native harness; it does not add an automatic rollback API, change marker semantics or embed application logic.

## Reproducible evidence

Current phase root: `/home/jason/qa-evidence/str158-flink-baseline-20261027/`. Original expired-transaction cases: `/home/jason/qa-evidence/str158-reproduction-20261027/`. Each case retains raw broker, catalog, process and source/output records, command exits, source/package hashes and owned cleanup evidence.

Current native pin is normal dev with matching precompiled RocksDB, built through maintained `scripts/build-pin` and the shared `scripts/rust-build` queue from fetched main `b448819312c5a16f5b3815a993000497ca550e19` plus the PR #88 diagnostic increment. Executable SHA256: `9f070fadf0a2613c3a958ecbc87dbf848d47dfb9f666369ea196dd29a0a572f8`. Runtime image: `bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`. Detailed source/package receipts live in `dev-pin/`.

| Scenario | Evidence | Observed result |
| --- | --- | --- |
| Original short transaction expiry plus actual worker loss | `str158-reproduction-20261027/original-expired-1/` | Expected refusal reproduced: broker abort/epoch advance, original worker fencing, replacement restore of committing checkpoint and missing exact marker. |
| Operator completion delayed across unchanged 10 s test timeout | `str158-reproduction-20261027/checkpoint-expired-1/` | Synthetic test-only gate establishes operator/global-checkpoint delay path; both original transactions expire before release; replacement refuses. Not historical production causality. |
| Incomplete checkpoint, native RocksDB state, actual worker loss | `inprogress-state-rocks-1/` | PASS: original C is still inprogress immediately before SIGKILL, automatic P restore, old C excluded, replacement same numeric C with new public ID becomes ready; exact native state/output and fresh suffix, 65 total rows per sink, physical prefix preserved. |
| Existing 60,000 ms timeout, synthetic 65 s post-global hold | `prevention-60s-65s-1/` | Expected refusal reproduced: both transactions expire, original producer is fenced, actual worker is killed, replacement restores pending checkpoint and refuses; committed prefix/markers stay unchanged. |
| Explicit 900,000 ms timeout, same synthetic 65 s hold | `prevention-900s-65s-1/` | PASS: transactions remain Ongoing during hold, original worker commits exact markers and processes exact 65/65 native outputs/fresh suffix. Observed-ready lifetime upper bounds 66.857/67.048 s. |
| Explicit 900,000 ms timeout, actual worker loss before pending commit | `prevention-900s-before-1/` | PASS: replacement resumes exact pending identities, commits once and processes exact 65/65 native outputs/fresh suffix; physical prefix preserved. Observed-ready lifetime upper bounds 32.718/32.608 s. |

The incomplete-checkpoint case keeps the ordinary fixture's declared 120 s transaction timeout, while the two expiry reproductions retain their original 10 s setting. These are explicitly different fault windows. No pacing or wall-clock application substitution is used. The generic caller-supplied native SQL fixture exercises MERGE/state-table/lookup behavior; external application preparation and application oracles remain outside Streamr.

## Remaining qualification

The matched 65 s controls and normal pending-commit process-loss case passed on one frozen harness/fixture/native pin. They measure broker transaction start through observed-ready time as an upper bound on commit acknowledgement, not an exact broker commit timestamp. Independent raw batch audit passed, including exact transaction identities, native state/output oracles, source/package binding and absence of all owned runtime resources. The 65 s normal-commit case and 32.718 s recovery case are separate fault windows; a combined 65 s hold plus crash was not run. The broker's existing maximum 900,000 ms has been captured. Prepared SQL/program timeout-only changes are independently reviewed, reversible byte-for-byte and unapplied. They do not establish coverage for a 15 minute outage or original production timeout causality.

Already failed production committing checkpoints require a separately qualified, exact external operation. Candidate qualification includes complete immutable P/C archives, owner quiescence, resolution of both exact producer slots, a complete read_committed marker scan through physical highwater, exact original checkpoint-public-ID catalog comparison/update, existing public safe restart, native replay and fresh output, plus crash/refusal branches. An exact C marker always takes ordinary acknowledgement without checkpoint reclassification. The candidate preserves committed state and output at P while replaying the uncommitted suffix; new UUIDs and intermediate results in that suffix may differ from abandoned C. No marker writes, force acknowledgements, ignore_state resets or automatic general rollback are introduced.

Production recovery, application validation and deployment are pending and coordinated with the integration owner. A diagnostic or incomplete-checkpoint test pass does not resolve the two existing failed jobs. Final source/evidence review and current-head PR CI remain required for delivery.
