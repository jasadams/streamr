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

## One-time failed-checkpoint qualification

The eight finite operation cases all exited 0 and passed independent source and raw-evidence review. They use one externally supplied generic native SQL fixture, one transactional Kafka sink/subtask, native MERGE plus lookup, UUID state bindings and four declared input rows. No application schema, UDF or engine recovery API was added.

| Evidence case | Result |
| --- | --- |
| `one-time-rocks-1/` | Expired C fails normally; archive P/C, resolve both exact producer slots, scan exact P through physical high, perform guarded catalog/config operation, public safe restart once, native P replay/new C/fresh UUID state all pass. |
| `one-time-memory-1/` | Same operation passes with memory live state. |
| `one-time-after-cas-1/` | Actual controller loss after catalog commit leaves Failed/equal-nonce terminal across restart; read-only already-applied recognition then one public restart succeeds. |
| `one-time-replay-inprogress-1/` | Replacement worker killed while new C public-ID row is inprogress; original C global bytes can still exist. Normal recovery selects P, excludes the incomplete replacement and commits a further distinct C public ID without publishing its abandoned physical record. |
| `one-time-replay-committed-1/` | Worker killed after replacement C marker/output commit but before catalog ready; exact saved producer resume/marker acknowledgement avoids duplicate output. |
| `one-time-exact-c-1/` | Both-slot resolution ends at exact original C marker. Catalog/program stay unchanged and normal C acknowledgement succeeds; no catalog apply, timeout patch or public restart is invoked by this branch. |
| `one-time-cp312-rocks-1/` | Full operation passes using the exact production Python 3.12.5 executable and SQLite 3.46.0 for both Kafka and catalog CLIs, with pinned cp312 client/native 2.3.0. |
| `one-time-source-retention-refused-1/` | Owned isolated source is trimmed to low 2 above native P cursor 1. Operation refuses before producer resolution, catalog apply or restart; all five relevant catalog rows/program/nonces and committed prefix/marker remain unchanged. Four inputs were declared, two published, one committed and two fresh inputs withheld. |

All successful replay cases check four native output rows and fresh state updates. The committed P UUID binding remains fixed; the abandoned C UUID actually regenerates in the P replay cases and agrees with later native lookups. This is a deliberate uncommitted-suffix change, not preservation of abandoned C values. Archives are copied without hardlinks, independently inventoried against native descriptors, hashed before and after numeric C path reuse, and retained outside live cleanup. Every owned process/container/network/broker volume is verified absent after each case.

The external tools are source-bound operational artifacts, not an engine service. The Kafka helper only initializes two supplied transaction IDs and performs a bounded read_committed scan; the catalog helper compares complete expected rows and atomically changes the exact original C public-ID row plus the prepared timeout-only program/SQL bytes. It never edits restart nonces/status, markers, checkpoint files or source positions. Exact C takes ordinary recovery rather than C reclassification. Missing/newer/other-generation/incomplete markers, stale catalog/config or missing archives refuse. Pure helper tests cover additional input guards; fake clients do not count as live broker evidence.

The closure validator deliberately supports the format 2 disk/typed snapshots used by these targets; an unsupported format refuses even if a native reader could support it. It is an integrity check, not a replacement for actual native schema/state restoration. The earlier proposed engine recovery framework is excluded; restoration uses the existing controller and native state interfaces.

## Production preparation and limits

The matched 65 s controls and normal pending-commit process-loss case are separate fault windows. Broker-start through observed-ready times are upper bounds on commit acknowledgement, not exact broker commit timestamps. Independent audits verified source/package binding and cleanup. A combined 65 s hold plus crash was not run. The captured broker maximum is 900,000 ms; the candidate does not establish coverage for every 15 minute outage or historical production timeout causality.

Read-only preparation preserved 2,474 production checkpoint files / 462,780,627 bytes: complete P4456/C4457 and P1016/C1017 reference closures and both original C subtrees. Source/copy/native checksums, owner paths, pod binding, single-link/read-only copies and complete inventories passed independent audit. No catalog, producer, source, marker, timeout or restart mutation was performed in production. The production interpreter hash exactly matches the separately qualified Python 3.12.5 runtime; no package was installed in production.

The candidate preserves committed state/output at P and reprocesses the uncommitted interval through the existing native SQL. UUIDs, tied ordering and intermediate window/CDC results in that abandoned suffix may differ. Identity 4457 and profiles 1017 remain unresolved until the exact production operation is explicitly approved and coordinated. Fresh input eligibility, exclusive ownership, full current source/marker proofs, consistent online catalog backup and exact row/hash/nonces are refreshed execution gates; old captures alone do not authorize repair. Source catch-up and genuinely fresh downstream output must be distinguished from materializing old committed backlog.

The currently deployed source is `597b6326` in namespace `data-pipeline`. Main b448 adds STR161 empty-directory pruning; recovery/SQL/catalog interfaces are otherwise unchanged except opt-in diagnostic hooks in this PR. No engine repair or new image requirement was demonstrated. Production action remains separate from this reviewed test increment; full-ticket completion and merge are not claimed by the isolated results.
