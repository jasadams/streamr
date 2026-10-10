# STR-32 checksum recovery admission

Concurrent RocksDB recovery could reject a checksum buffer behind queued namespace scans in the fair shared scan-memory pool. The checksum pass now awaits its bounded 64 KiB allowance before acquiring decoded-state or write memory. Cancellation removes the waiter; pool limits and checkpoint format are unchanged. The user approved this behavior change.

Validated on fetched base `52391450bc055704c38112967c73265e9bafe015` plus the checkpoint repair under Bookworm dev image `a95d0ca27fc16ae428c9b0df8d02b603dacab71ebf3a915aad85c94b422cf9ab`:

- `cargo test --locked -p arroyo-state --lib live:: -- --test-threads=1`: 92 passed, including actual-Parquet contention, cancellation and fresh retry.
- Locked affected-crate check, Clippy `--no-deps --all-features --all-targets -D warnings`, and workspace formatting passed.
- Rebuilt product SHA256 `47b4ddd67bc733e842a338034fdcb67d46ab5a01fd21f7521740cc77853993b9`: four fresh combined RocksDB cases, controller/leader × source batch targets 1/8, passed actual worker SIGKILL recovery. Retained checkpoint 2, source prefix two and all sink offsets were verified. Exact aggregate/calendar/SESSION/table outputs passed with one replacement generation and zero scan admission failures. Independent source and evidence reviews approved.

The baseline controller batch-1 case needed three extra recovery generations; leader batch-1 timed out after an admission refusal. Original failures and repaired receipts remain at `/home/jason/qa-evidence/str32-20261010-52391450/`. Repo-accessible detailed receipts are in the invoking checkout's `docs/str-32-recovery-repair-results.json`.

These are finite five-row functional recovery checks. Checkpoint barriers can flush partial source batches; batch target 8 is not evidence of an actual eight-row batch. This does not establish capacity, broader production faults, backfill or 24-hour qualification. STR-32 remains open.
