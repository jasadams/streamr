#!/usr/bin/env bash
set -euo pipefail

# Run inside the Bookworm development container, with the pinned Arcstream
# reference worktree mounted read-only. This verifies the first implementation
# slice; profile/session readiness and the 24-hour gate remain separate.
reference_root="${ARCSTREAM_REFERENCE_ROOT:?Mount the pinned Arcstream reference and set ARCSTREAM_REFERENCE_ROOT}"
reference_tools="$reference_root/test/streamr-reference"
test -f "$reference_tools/streamr_capture.py"
test -f "$reference_tools/compare_identity.py"

cargo test --locked -j4 -p arroyo-state -p arroyo-rpc -p arroyo-worker --lib
cargo test --locked -j4 -p arroyo-sql-testing --no-run

for mode in memory controller leader; do
    backend=rocksdb
    checkpoint_mode="$mode"
    if [[ "$mode" == memory ]]; then
        backend=memory
        checkpoint_mode=controller
    fi
    capture_directory="$PWD/target/milestone3-identity/$mode"
    python3 "$reference_tools/streamr_capture.py" prepare "$capture_directory" \
        --runtime-directory "$capture_directory" \
        --fixture "$reference_root/flink/identity-resolution/src/test/resources/reference/identity-input.json"
    STREAMR_TEST_EXECUTION_BYTES=16777216 \
        STREAMR_TEST_BACKEND="$backend" STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" \
        STREAMR_IDENTITY_QUERY="$capture_directory/query.sql" \
        STREAMR_IDENTITY_OUTPUT="$capture_directory/streamr-internal.jsonl" \
        cargo test --locked -j4 -p arroyo-sql-testing arcstream_identity_capture \
            -- --ignored --test-threads=1 --nocapture
    for phase in initial recovered; do
        input="$capture_directory/streamr-internal.jsonl"
        if [[ "$phase" == initial ]]; then
            input="$capture_directory/streamr-internal.initial.jsonl"
        fi
        python3 "$reference_tools/streamr_capture.py" adapt "$input" "$capture_directory/$phase.jsonl"
        python3 "$reference_tools/compare_identity.py" \
            "$reference_root/flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl" \
            "$capture_directory/$phase.jsonl"
    done
done

cargo check --locked -j4 -p arroyo-worker -p arroyo-state -p arroyo-rpc -p arroyo-sql-testing --all-targets
cargo clippy --locked -j4 -p arroyo-worker -p arroyo-state -p arroyo-rpc -p arroyo-sql-testing --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
git diff --check
