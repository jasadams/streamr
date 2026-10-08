# STR-38 acceptance evidence

Audited 2026-10-08 against implementation commit
`9bfb7df8aa8d016cfd07e49d59ebb2112611a09e` in
[PR #6](https://github.com/jasadams/streamr/pull/6).
This is the state-table DDL/catalog/execution-contract ticket, not full
milestone 3 qualification. No new engine behavior was needed during this audit.
The documentation correction uses the existing configuration key
`worker.sql-state-backend`; its default is `memory`, with `rocksdb` opt-in.

## Acceptance map

| Requirement | Implementation and executable assertions |
| --- | --- |
| Typed DDL, composite primary keys, partition compatibility, catalog separation and deterministic diagnostics | `crates/arroyo-planner/src/state_tables.rs` and the provider in `src/lib.rs`. Passing `state_tables::tests` include `composite_keys_and_stable_identities`, `declared_partition_compatibility`, `deterministic_declaration_diagnostics`, `catalog_is_distinct_from_intermediate_relations` and the quoted/dotted name collision tests. Assertions cover explicit nullable keys, unsupported key types, invalid ownership and unsupported declaration options. Named mutation-result tests distinguish retained storage from outputs. |
| Documented ownership, visibility, limits, compatibility, retention and parallelism | [State-table SQL contract](state-tables.md) and [storage contract](state-table-storage.md). These specify fixed parallelism 1, read-after-write visibility, bounded key/value/scope/output admission, schema/encoding checks, indefinite retention until deletion, no aggregate TTL and no silent backend fallback. `live::typed_table::tests::typed_table_backend_conformance` passed for memory, RocksDB and a forwarding adapter. |
| Event-driven current-row lookup, without retaining input history | `continuous_merge.rs`, `state_table_fusion.rs` and worker `arrow/state_table_owner.rs`. Passing planner tests include `inner_and_left_join_are_current_row_keyed_lookups`, `composite_key_requires_each_equality_once_and_no_residual` and `state_table_cannot_be_scanned_without_event_keyed_join`. Each event reads the current keyed row; later state changes do not revise prior events. |
| Serial input-event reads/writes/output capture and checkpoint replay | Worker `arrow/state_table_owner.rs` and `state_table_runtime.rs` execute dependent steps through the same scope before advancing to the next event. Passing tests include `serial_owner_reads_its_pending_writes_for_one_and_many_row_inputs`, `process_batch_emits_single_row_without_followup_and_splits_hot_multirow_input`, `concat_refusal_prevents_later_state_access_and_releases_permits_for_retry` and `blocked_collector_stops_next_chunk_and_cancel_releases_output_permits`. Four native SQL recovery captures below exercise the contract. |

Per-event ordering is separate from durable flush boundaries. Several serial
events can share a bounded batch scope; this introduces no per-row durable
transaction. The existing Arroyo checkpoint/recovery machinery remains in use.

## Executed evidence

Both Rust workflows for the implementation commit passed **715 library tests**
(six skipped) and **12 integration tests**; all seven PR checks were green.
[PR workflow](https://github.com/jasadams/streamr/actions/runs/37717960845)
and [push workflow](https://github.com/jasadams/streamr/actions/runs/37717957208)
retain the commands and named test results.

The local Bookworm v16 validation separately passed formatting, workspace
all-target check, strict Clippy, library tests (**714 passed, zero failed,
six ignored**) and the all-target build. Its manifest and pinned executable
lineage were independently reviewed. The relevant planner, worker, state and
configuration sources match both that compiled-source manifest and the published
implementation commit. Local and CI counts are distinct evidence.

The generic operations query performs keyed lookups, dependent insert/update/
delete MERGEs and intervening lookups, capturing source/before/after values.
Its native SQL capture and fresh-worker checkpoint recovery passed under:

| Backend | Recovery protocol | Initial rows | Recovered rows | Committed checkpoint prefix |
| --- | --- | ---: | ---: | ---: |
| Memory | Controller | 161 | 161 | 80 |
| Memory | Leader | 161 | 161 | 80 |
| RocksDB | Controller | 161 | 161 | 80 |
| RocksDB | Leader | 161 | 161 | 80 |

Comparisons check complete typed rows, multiplicities and the committed prefix.
The fixture explicitly permits unordered output; these receipts do not prove
ordered sink delivery, large-state capacity or general exactly-once delivery.

Local receipts are under `target/native-m3-reviewed-repairs-v16-validation/`:
`source.json`, `run/validation-evidence.json`, `run/units.log`,
`run/operations-batch1-unordered-results/measurements.json` and
`review-actual-evidence.md`. The validation receipt SHA-256 is
`bb983a58329d8df43e95b02c92c430ad3b62d04f7ef6e3fada770746c2daa857`.
The detailed audit, per-case capture hashes and independent STR-38 review are
under `target/str38-acceptance/`. These local artifacts supplement the retained
CI logs; they are not committed build artifacts.

## Delivery boundary

STR-38 can proceed to review after independent acceptance review and publication
of the documentation correction. Keep it separate from the larger-state,
compatibility, legacy removal and full milestone qualification tickets.
No new SQL syntax, backend read-view lifetime, clock or checkpoint contract was
approved or implemented by this audit.
