#!/usr/bin/env bash
set -euo pipefail

# Run generic engine verification inside the Bookworm development container.
# Application fixture preparation and business oracle comparisons run externally.

cargo test --locked -j4 -p arroyo-state -p arroyo-rpc -p arroyo-worker --lib
cargo test --locked -j4 -p arroyo-sql-testing --no-run

cargo check --locked -j4 -p arroyo-worker -p arroyo-state -p arroyo-rpc -p arroyo-sql-testing --all-targets
cargo clippy --locked -j4 -p arroyo-worker -p arroyo-state -p arroyo-rpc -p arroyo-sql-testing --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
git diff --check
