# STR-159: one orphan traversal per job

The experiment did not meet the user's 10–20× completed-application-throughput target. The observed matched pair improved 1.5446×. Recommendation: reject this change as the proposed 10× solution; preserve the unmerged candidate and evidence for the user's decision. No merge, release or production mutation occurred.

## Source and mechanism

- BEFORE: `fe2b44154e21887c4d2953c362c822e7a57c8511`.
- Candidate: `fdcca282185905645ca5b29cfaa85658d68c643d`.
- Arcstream harness: `79673406028cdfa817a756009471a02831c2f658` (base `b173ca1bf84d19877d38021f64231789dcefad90`).
- CLI SHA256 BEFORE `5d4e5d9e991839e0d4c4caf3de80b5db04a8d71a1028b5428bcb5854fddcdcd9`; AFTER `208c879eac318d78e227971bb1250f630d8b8b18cd860106c5058ebe126f7c2a`.

Known-file cleanup now completes for all declared operators before one job-prefix orphan traversal. Retained owner sets move into the shared pass, including zero-state owners. Old metadata remains until that pass succeeds; obsolete metadata deletion remains idempotent and the retained-minimum write remains last. The obsolete operator-level API and its two SQL-smoke callers were replaced by the existing job checkpoint interface. No RocksDB scan was added or changed; filesystem enumeration falls from one per operator to one per job cleanup. No checkpoint protocol, SQL, clock, backend budget, pacing or application behavior changed.

## Matched result

| Measure | BEFORE | AFTER |
| --- | ---: | ---: |
| Completed application DATA events | 5,000 | 5,000 |
| Credited processing seconds | 106.851591 | 69.177545 |
| Completed events/s | 46.793875 | 72.277789 |
| Engine process CPU seconds | 297.21 | 136.30 |
| CPU ms/completed event | 59.442 | 27.260 |
| Controller CPU seconds | 165.93 | 11.83 |
| Profile worker CPU seconds | 105.31 | 103.61 |
| Sampled summed process RSS bytes | 809,914,368 | 803,717,120 |

Controller CPU fell 92.9%, while profile-worker CPU fell only 1.6%. The measured cleanup mechanism worked; the remaining profile processing work was essentially unchanged. The requested 10× improvement was not demonstrated.

Both arms used release/precompiled-native builds, the same immutable image/native archives/UI assets, 4 CPU and 8 GiB, RocksDB and unchanged application resource settings, source batch 512/linger 100ms, 2s checkpoints and 60s Kafka transaction timeout. Independent inventory comparison found exactly four approved compiled-file changes and 1,341 equal compiled inputs.

Each arm executed all five current application SQL queries, the qualified staged event-time sentinel, 4,998 new-person seeds, public checkpoint-stop/resume with fresh workers and unchanged committed warm outputs, and one unpaced 5,000-event tail. Completed work required a common raw DATA prefix joined through actual identity/unified offsets, all five completed source frontiers and exact committed profile images. Both arms stopped on full completion before the 599.5s cap; no later commit was credited.

Independent audits verified 208 unique timed source-checkpoint proofs directly from preserved Parquet; all raw/control offsets; 10,009 raw and 10,008 accepted events per arm (one declared late event rejected); all 10,011 complete 22-field profile images; both closed SESSION outputs; identity bindings and empty merges. Process/container/network/anonymous-volume cleanup passed before queue release. These are actual native application outputs, without application UDFs or an alternate processor.

## Commands and evidence

Evidence root: `/home/jason/qa-evidence/str159-single-pass-20261026/`.

Maintained dev gates (normal dev profile, matching precompiled archive):

```sh
scripts/cargo-dev fmt --all -- --check
scripts/cargo-dev test --locked -p arroyo-state parquet::disk_cleanup_tests -- --test-threads=1
scripts/cargo-dev test --locked -p arroyo-state live::checkpoint::tests -- --test-threads=1
scripts/cargo-dev check --locked -p arroyo-state -p arroyo-sql-testing
scripts/preflight-clippy.sh -p arroyo-state -p arroyo-sql-testing
scripts/rust-build python3 /home/jason/qa-evidence/str159-single-pass-20261026/smoke-gates.py
```

Results: six cleanup tests passed (the historical opt-in STR-156 benchmark was not run), 18 checkpoint tests passed, both affected controller SQL smokes passed with literal recovery/lifecycle outputs, and locked check/fmt/strict Clippy passed. Initial Clippy found a test-only guard-scope issue; lexical scope fixed it and the focused recheck passed. Arcstream fixture/runtime/pair unit tests passed. Three independent pre-code reviews and independent code, fixture, runner, measurement and artifact reviews are saved in the evidence root.

The actual optimized invocation was:

```sh
scripts/rust-build env STR159_CANDIDATE_COMMIT=fdcca282185905645ca5b29cfaa85658d68c643d python3 /home/jason/qa-evidence/str159-single-pass-20261026/optimized-pair.py
```

Queue job `813f6aa7a428` covered both `scripts/build-pin --profile release` builds, candidate wire-only preflight, one BEFORE/AFTER pair and owned cleanup. `optimized-execution.json` records every exact argument, source checkout, elapsed time and exit; all four phases exited zero. Package manifests preserve source/native/linkage/executable hashes outside Cargo caches. Both release builds, actual stop/resume preflight and run cleanup passed. No repeat campaign or profiler was run.

Primary receipts: `result.json`, `independent-final-measurement-verdict.json`, `independent-final-application-measurement-verdict.json`, `independent-package-audit.json`, `independent-source-proof-control-audit.json`, `matched-pair/pair-result.json`, both runtime rate histories and CPU samples, and all cleanup receipts.

## Limits and remaining decision

- This is one nonrandomized matched application fixture with synthetic immediate checkpoint-directory children. Actual nested directory/file history differs from production; start counts/nesting and subsequent real checkpoint additions are recorded. It is not a production replay throughput guarantee or a regression claim.
- Host load differed: BEFORE one-minute load 3.73→7.75, AFTER 3.51→3.34. A precise general causal speedup is not established. The observed pair is clearly below 10×.
- CPU uses stable PID/start counters through the frozen boundary (66.47ms BEFORE, 10.24ms AFTER beyond the credited endpoint). RSS is sampled summed per-process residency, with actual maximum sampling gaps 3.990s BEFORE and 2.048s AFTER; cgroup memory high-water can include preload/page cache/observer memory.
- The frozen harness had a quadratic offline capture-validation loop. It delayed test completion outside both credited processing/CPU boundaries; the same frozen harness was used for both arms. This is a harness inefficiency, not an engine performance result.
- The existing STR-126 larger-than-memory/full recovery qualification was reused; no 1.2M-event campaign was repeated. This experiment does not qualify production cutover, FlareDB ingestion throughput or deferred application features.
- Hosted PR CI was not run because the experiment remains unpublished. Local maintained gates passed. Keeping/merging this partial improvement or selecting another hypothesis requires the user's next decision; no automatic rollout or second experiment follows.
