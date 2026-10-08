#!/usr/bin/env bash
set -euo pipefail
# Execute inside the Bookworm development container from the repository root.
cargo test --locked -j4 -p arroyo-state -p arroyo-state-protocol -p arroyo-rpc -p arroyo-planner -p arroyo-worker --lib
cargo test --locked -j4 -p arroyo-sql-testing stateful_processor -- --test-threads=1
for checkpoint_mode in controller leader; do
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" \
    cargo test --locked -j4 -p arroyo-sql-testing stateful_processor -- --test-threads=1
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" STREAMR_TEST_CRASH=1 \
    cargo test --locked -j4 -p arroyo-sql-testing stateful_processor_operations -- --test-threads=1
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" STREAMR_TEST_CHECKPOINT_STOP=1 \
    cargo test --locked -j4 -p arroyo-sql-testing stateful_processor_shared_ctes -- --test-threads=1
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" \
    cargo test --locked -j4 -p arroyo-sql-testing smoke_tests::fault_tests -- --ignored --test-threads=1
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" STREAMR_TEST_CRASH=1 STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=600 \
    cargo test --locked -j4 -p arroyo-sql-testing milestone2_larger_than_ram -- --ignored --test-threads=1 --nocapture
 done
cargo check --locked -j4 -p arroyo-worker -p arroyo-planner -p arroyo-state -p arroyo-state-protocol -p arroyo-rpc -p arroyo-controller -p arroyo-sql-testing --all-targets
cargo clippy --locked -j4 -p arroyo-worker -p arroyo-planner -p arroyo-state -p arroyo-state-protocol -p arroyo-rpc -p arroyo-controller -p arroyo-sql-testing --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
git diff --check
