# STR-161 historical empty-directory cleanup validation

Implementation source base: `597b6326ab82f9c70634fe3e241f0c355f8e72cf`.
Plan: [str-161-cleanup-plan.md](str-161-cleanup-plan.md), reviewed SHA256
`61c30f93bb0fe7f6e7113c76c0e8baa471c4cc99bc861ddd5e825f480d11a04f`.
Three independent pre-code approvals:
`/home/jason/qa-evidence/str161-cleanup-20261027/plan-lifecycle.md`,
`plan-storage.md`, `plan-performance.md`.

## Implemented scope

Local storage exposes a serial asynchronous postorder empty-directory prune.
It qualifies paths once, preserves the requested prefix and configured root,
checks static prefix/ancestor symlinks and does not descend child symlinks.
Only `remove_dir` mutates storage. Missing and nonempty-directory races are
benign; other errors retain path context and propagate. Active frames and
open directory handles are O(depth), with no historical inventory or detached
full-tree task. This retains the existing trusted-root assumption, not an
adversarial concurrent-symlink sandbox.

The private checkpoint cleanup seam invokes pruning immediately before the
unchanged complete job-prefix orphan listing. Known-file, owner, reference,
epoch, metadata deletion and publication rules remain intact. Future
post-prune deletions can leave empty directories until the next cleanup;
no persistent cache/cursor skips late uploads. Cloud backends do no extra work.
No new RocksDB scan, checkpoint schedule, unchanged SESSION export, Kafka,
SQL, resource admission or application behavior change.

The explicit tradeoff is a second directory traversal of current live entries.
An ignored finite test compares zero-history and 2,000-history fixtures,
first pass separately from eight alternating steady pairs, with exact immutable
key/byte oracles and existing inotify selected-directory-open instrumentation.
A private test-only SkipPruning adapter supplies the unchanged baseline.
No production flag or dependency was added. Selected opens are not whole-tree
syscall counts; test timings do not establish production or engine throughput.

## Source and environment

Evidence directory: `/home/jason/qa-evidence/str161-cleanup-20261027/`.
`tested-inputs.json` records executable inputs before all gates and the runner
asserts identical hashes afterward. Manifest SHA256:
`b9c8ef3615e4662b31030e925ad775a5721179e5179d8962d49067b370e689d4`.
The reviewed plan hash above is unchanged.

| Executable source | SHA256 |
| --- | --- |
| `crates/arroyo-storage/src/lib.rs` | `f5f49afa54629ae78c65131ac210e86f9b53f76204a15249c12ef2357b05c2ac` |
| `crates/arroyo-storage/src/local_cleanup.rs` | `a3a55bb22bb1813a18c0dba79932d4caf490fce4f3e6afcdfd0438462b0571fb` |
| `crates/arroyo-storage/src/local_cleanup/tests.rs` | `3840b7c7a141ed585c87fa8b0d8fb68e547ffc39e0f1896324873c891b32e738` |
| `crates/arroyo-state/src/parquet.rs` | `e045f2142508606e498648df38c55d15c746c7d729b0873380a32adf34f76cf9` |
| `crates/arroyo-state/src/parquet/disk_cleanup_tests.rs` | `ce8018554dda6316dcb240d27399f665392b1a08bd58de7d557590c859e45708` |

All finite build/test commands ran foreground, serialized in the maintained
`scripts/rust-build` shared queue through maintained wrappers. Purpose was
focused correctness/recovery and finite filesystem work. Actual profile:
normal dev/test (unoptimized test binaries), shared `target/milestone2-runtime`.
The selected maintained dev image was
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`;
matching precompiled RocksDB mode was selected by the runner, as recorded in
`environment-repairs.json` and gate logs. No release/source-native build or
separate target was used.

Two initial queue jobs (`b9c062709d3d`, `4d1a7e2f8ad4`) exited 1 before any
commands because disk reserve/budget remained unmet; one observed boundary was
99.4 GiB free after automatic cleanup. Coordinator trimmed old compiler-cache entries under the shared build lock
and retried the maintained queue, preserving `compiler-cache-cleanup.json`
with the receipt for 3,441,858,755 bytes of regenerable host sccache entries older than
a day. A subsequent default-image attempt failed before product compilation:
fmt/check exited 2 and preflight Clippy exited 1 because the image lacked
matching precompiled RocksDB. These records remain under `initial-image-failure/`.
Selecting the maintained image above resolved that prerequisite. None of these
attempts is a failed or passing executable test.

## Maintained gates and correctness results

Every final command below exited **0**. Exact command arrays, exits and elapsed
times are in `gate-results.json`; named logs retain full output.

| Command | Result | Log |
| --- | --- | --- |
| `scripts/cargo-dev fmt --all -- --check` | Formatting passes | `fmt.log` |
| `scripts/cargo-dev check --locked -p arroyo-storage -p arroyo-state` | Affected crates check passes | `check.log` |
| `scripts/preflight-clippy.sh -p arroyo-storage -p arroyo-state` | Strict Clippy passes | `clippy.log` |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-storage --lib` | 11 passed, 0 failed, 0 ignored | `storage-tests.log` |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-state parquet::disk_cleanup_tests -- --test-threads=1` | 7 passed, 0 failed, 2 ignored | `cleanup-tests.log` |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-state live::checkpoint::tests -- --test-threads=1` | 18 passed, 0 failed, 0 ignored | `checkpoint-tests.log` |
| `scripts/cargo-dev test --locked --no-fail-fast -p arroyo-state parquet::disk_cleanup_tests::historical_empty_directory_work_comparison -- --ignored --exact --nocapture --test-threads=1` | 1 passed, 0 failed (explicit ignored test) | `comparison.log` |

There are **36 passing correctness tests plus one passing finite comparison**.
The checkpoint suite includes an internal child-process invocation of one test;
it is part of the reported 18, not an extra unique test. The cleanup suite's two
default ignores are the older STR-156 release comparison and the new STR-161
comparison. The latter subsequently ran explicitly and passed; STR-156 remains
ignored and is outside this increment. All selected test commands use
`--no-fail-fast`; no executable failure was observed or repaired.

Passing checks cover actual local multipart staging, completion and reupload
after parent pruning; root/key/prefix/neighbor byte preservation; static
child/prefix/ancestor symlinks; real filesystem error propagation; prune-specific
failure before listing and metadata publication; observed partial directory
pruning followed by cancellation and fresh-provider retry; repeated cleanup;
late old uploads; ownership, retained references and active epochs. Existing
checkpoint export/restore checks remain passing. Host formatting and staged
whitespace checks also passed, followed by the maintained formatting gate above.

## Finite filesystem comparison

`comparison.log` retains all observations; `comparison.json` is the raw receipt
and `comparison-summary.json` contains derived summary values. Both arms use the
actual orphan cleanup with identical ownership/retained references. Baseline
suppresses only pruning through the private test adapter. Each independently
constructed arm has ten immutable objects, an unrelated job and either zero or
2,000 historical checkpoint trees with one operator and two table directories.
First pass is separate from eight steady pairs, alternating baseline/candidate
order. Setup, inventories, inotify setup/drain and independent full byte/key
oracles are outside cleanup timing. CPU is process user+system time and can
exceed wall time due to filesystem helper threads.

The first code review requested one instrumentation repair: the initial
inventory opened watched directories before measurement. Both watches now drain
immediately after that inventory, preserving exact assertions and per-pass
oracle boundaries. No earlier comparison result exists. `code-review-2.md`
approves the repaired source; the following results are from its exact hashes.

All numbers below are milliseconds. First-pass values are individual
observations; steady values are median [minimum–maximum] across eight passes.

| History epochs | Arm | First wall | First CPU | Steady wall | Steady CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | baseline | 1.122 | 1.021 | 0.736 [0.613–0.886] | 0.698 [0.631–0.807] |
| 0 | candidate | 2.602 | 2.743 | 2.184 [1.981–2.884] | 2.308 [2.154–2.883] |
| 2,000 | baseline | 143.514 | 142.859 | 150.361 [144.731–169.103] | 149.395 [143.953–166.341] |
| 2,000 | candidate | 826.519 | 912.226 | 2.992 [2.172–4.153] | 3.036 [2.319–3.821] |

With historical trees, both arms start with 8,026 directories. Candidate removes
8,000 empty directories on its first pass, leaving 26; baseline retains 8,026.
These counts remain unchanged through all eight steady passes. The selected
historical checkpoint directory opens once on baseline's first and every steady
pass; candidate opens it once during the first prune, removes it, and records
zero subsequent opens. Zero-history arms both retain 26 directories. Every
first/steady observation passes the complete ten-object key/byte oracle and
preserves the unrelated job's empty subtree.

This directly demonstrates removing recurring historical-directory work, with
an expensive first prune. It also demonstrates a cost on the no-history control:
the extra live-directory traversal raises median wall time from 0.736 to 2.184 ms
and median CPU from 0.698 to 2.308 ms. No policy hides that tradeoff.
Inotify measures the selected directory, not whole-tree syscall work. No strace
run occurred. These normal test-profile local timings establish neither
production CPU reduction nor end-to-end or optimized engine throughput.

## Review and delivery boundary

Three independent plan reviews approved the original exact plan; second source
review approves the repaired source. Independent `measurement-review.md` gives
**KEEP** after directly reconciling raw log JSON, all five source hashes, gate
logs and per-pass arithmetic. Its arithmetic means (historical steady wall
152.344 → 3.120 ms and CPU 151.124 → 3.054 ms) are consistent with the explicitly
labelled medians above. The audit retains the expensive first pass and
zero-history regression and limits KEEP to this scoped filesystem repair.
Final integrated source/evidence review **APPROVE**, with no findings, is
recorded in `final-review.md`. Publication and current-head PR CI are delivery
gates; the ticket and PR carry their live outcome.

No production mutation, deployment or full-engine/application campaign occurred.
Unchanged SESSION export remains unresolved, tracked separately in STR-162 and
related to existing STR-25. This increment does not alter that export or claim
to fix the complete stalled-stream CPU cost. No new RocksDB scan or checkpoint,
Kafka, SQL or application execution architecture was introduced.
