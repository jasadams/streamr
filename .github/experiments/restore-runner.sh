#!/usr/bin/env bash
set -euo pipefail
: "${STR163_RUN_ROOT:?}"
export STREAMR_BUILD_EVIDENCE_ROOT="$STR163_RUN_ROOT"
export CARGO_BUILD_JOBS=2
fixture="$STR163_RUN_ROOT/fixture"
results="$STR163_RUN_ROOT/results"
python3 .github/experiments/restore-fixture.py create "$fixture"
sha256sum "$fixture/manifest.json" >> "$results/controls.sha256"
for role in baseline candidate; do
  if [[ "$role" == baseline ]]; then
    revision=0af056f82f84d7c3ec273a5e6700f6a15e89bd97
  else
    revision=36818f61a34487880096c54fe76bbcae0812df77
  fi
  git show "$revision:crates/arroyo-storage/src/streaming.rs" > crates/arroyo-storage/src/streaming.rs
  git show "$revision:crates/arroyo-state/src/live/native_checkpoint.rs" > crates/arroyo-state/src/live/native_checkpoint.rs
  python3 .github/experiments/restore-overlay.py
  sha256sum crates/arroyo-storage/src/streaming.rs crates/arroyo-state/src/live/native_checkpoint.rs > "$results/$role-source.sha256"
  scripts/cargo-dev test --locked --release -p arroyo-state --lib local_native_download_measurement --no-run --message-format=json > "$results/$role-build.jsonl" 2> "$results/$role-build.stderr"
  executable=$(python3 .github/experiments/restore-executable.py "$results/$role-build.jsonl")
  scripts/cargo-dev --exec sha256sum "$executable" > "$results/$role-executable.raw"
  grep -E '^[0-9a-f]{64}  /app/target/' "$results/$role-executable.raw" > "$results/$role-executable.sha256"
  test "$(wc -l < "$results/$role-executable.sha256")" -eq 1
  python3 .github/experiments/restore-fixture.py warm "$fixture" > "$results/$role-warm.txt"
  cat /proc/pressure/io > "$results/$role-pressure-before.txt"
  scripts/cargo-dev --exec env "STR163_RESTORE_FIXTURE=$fixture" /usr/bin/time -v -o "$results/$role-time.txt" timeout --signal=TERM --kill-after=10s 600s "$executable" live::native_checkpoint::tests::local_native_download_measurement --ignored --exact --test-threads=1 --nocapture | tee "$results/$role-test.log"
  grep -F 'test result: ok. 1 passed; 0 failed' "$results/$role-test.log"
  grep -F 'STR163_OUTPUT_ORACLE=PASS' "$results/$role-test.log"
  cat /proc/pressure/io > "$results/$role-pressure-after.txt"
done
