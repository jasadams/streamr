# STR-156 — job-scoped checkpoint orphan cleanup

The controller previously recursively listed the entire configured checkpoint
store once per operator, then filtered to the owning job. The repair passes the
owning job's `checkpoints` prefix to the existing object-store listing. The pinned
`object_store 0.12.3` local implementation starts its filesystem walker at that
prefix, so unrelated jobs are excluded before enumeration.

Deletion predicates, retained-file membership, strict epoch cutoff, idempotent
deletes, concurrency and checkpoint publication/recovery remain unchanged.
Operators still repeatedly traverse the complete owning-job subtree, including
empty/retained/new epochs and other operators. No RocksDB scan, cache, resource
budget, SQL behavior or application schema changed.

## Source and review

Fresh fetched base: `d208beda8ea36389d198cf03ad992fea3ba8147a`.
Branch: `jason/str-156-cleanup-20261026`. Instructions: `AGENTS.md`,
`.claude/build-test.md`, `.claude/team.md`, current milestone ledger/feature plan,
and STR-156. The invoking dirty checkout and prior released preparation tree
were preserved.

Exact plan SHA256 `6f28b42353115a226a0abb3c0317b434e551f0c89a994ac05e1f7b81ef0f217e`
received three independent pre-code approvals: lifecycle, storage/recovery and
performance/measurement. Independent code review approved the source and tests
with no findings. Final executed evidence review is recorded with the delivery evidence.

## Validation evidence

Evidence root: `/home/jason/qa-evidence/str156-cleanup-20261026/`.
Normal dev/test checks use maintained scripts, shared target/queue and matching
precompiled native archives in immutable image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
The old default image refused the first formatting command (exit 2); selecting
the matching maintained image resolved that preflight refusal without source
changes or rebuilding the image.

All selected maintained dev gates exited 0:

- `scripts/cargo-dev fmt --all -- --check` (implementation formatting also passed).
- `scripts/cargo-dev check --locked -p arroyo-storage -p arroyo-state`.
- `scripts/preflight-clippy.sh -p arroyo-storage -p arroyo-state`.
- `scripts/cargo-dev test --locked -p arroyo-storage --lib --no-fail-fast -- --test-threads=1`: 8 passed.
- `scripts/cargo-dev test --locked -p arroyo-state --lib --no-fail-fast parquet::disk_cleanup_tests -- --test-threads=1 --nocapture`: 4 passed; one opt-in benchmark ignored here and executed explicitly in release below.
- `scripts/cargo-dev test --locked -p arroyo-state --lib --no-fail-fast live::checkpoint::tests -- --test-threads=1`: 18 passed, including its nested concurrent-write child test.

The interruption regression observed exactly 1 of 256 orphan deletions before
dropping the cleanup future; fresh-provider retry removed all 256 while retaining
the protected page. Inotify observed both target/empty-target directory opens
and no unrelated-job or neighboring-namespace opens. No selected test failed.
The original default-image formatting refusal is retained as a failed preflight,
not a passing check.

The deterministic tests retain the original cleanup regression and add exact
path/byte deletion oracles, old retained pages, equal/new epochs, other jobs and
operators, neighboring namespaces, malformed paths, empty/missing namespaces,
repeat cleanup, late upload and observed partial deletion followed by fresh
storage-provider retry. Linux inotify tests physical directory-open exclusion;
it is zero/nonzero evidence because events may coalesce. Configured-key tests
check qualification and upstream listing-error propagation.

## Bounded optimized comparison

One release ABBA campaign completed, with no rerun. Both test binaries used
`scripts/cargo-dev test --release --locked -p arroyo-state --lib --no-run`,
the same immutable image and matching prebuilt runtime/build-dependency archives.
The shared queue remained reserved through both builds, pinning, tracing,
processing and owned cleanup. Candidate build took 13m28s; baseline 29.58s.
Build/setup/audit are excluded from cleanup timings.

Both versions include the identical test harness and storage helper. The baseline
changes only the one cleanup-listing expression back to `storage.list(true)`.
Full 1,701-file source inventories differ only at `parquet.rs`; the recorded inverse
was restored exactly. No measurement overlay remains in production code.
Candidate executable SHA256: `1d59ae5df96ac5f21665b6f858e60a4b6cf95985994edb1c39724b68a104925e`.
Baseline: `e8301fc7ec175d77f7e2c6a6dd333a5b65b9d808e4cb0a410dde25149d2e2784`.
Image archive/build-lock/config hashes, actual Cargo native selection/fingerprint
closure and executable/runtime-library hashes are retained per pin.

The fixed fixture has 32 jobs, 16 populated epochs, four operators, two tables
and three files per table (12,288 files), plus 16 distinct empty directories per
job. Each arm completed all four target cleanup calls, deleted exactly 64 old
unreferenced disk pages, and preserved all remaining 12,224 paths and their bytes.
Old referenced pages and equal/new epochs remain protected. The 120-second
per-arm processing watchdog never fired; total measured processing was 0.6053s,
well below the 480-second campaign bound.

| Arm | Version | Cleanup elapsed (s) | Process user+system CPU (s) | Oracle |
| --- | --- | ---: | ---: | --- |
| 1 | Baseline | 0.285132 | 1.087500 | Pass |
| 2 | Candidate | 0.014213 | 0.046115 | Pass |
| 3 | Candidate | 0.011171 | 0.036993 | Pass |
| 4 | Baseline | 0.294781 | 1.141542 | Pass |

A separate small traced diagnostic excludes setup/audit and never supplies timing
observations above. Baseline recorded 14 directory-open syscall entries and 28
`getdents64` calls across 14 directories, including unrelated-job/empty directories,
the storage root and `checkpoints-extra`. Candidate recorded 5 directory-open
entries and 10 directory-read calls across the 5 owning-job checkpoint directories.
No unresolved directory descriptors or unfinished entries were observed. Inotify
independently provided baseline-positive/candidate-zero unrelated traversal.
These are syscall-entry/read-call counts, not counts of unique filesystem entries.
Raw phase-delimited traces remain preserved.

All arms inherited CPUs 0–7, four harness runtime threads and unlimited container
CPU quota; no resource settings differed. Host load was approximately 5.0–5.4 on
eight CPUs. Fixtures were freshly created and audited before timing, warming their
metadata; no global cache drop occurred. Boundary host/cgroup/pressure snapshots
and one-second outer histories are retained, but these subsecond cleanup windows
are shorter than the sampler interval. Two observations per version do not establish
statistical stability or a production capacity ratio. Both candidate elapsed/CPU
observations are below both baseline observations; the work-isolation mechanism
and complete correctness oracle support keeping the narrow repair.

`/proc/self/io` reported zero physical read bytes in all arms and is not metadata
traversal evidence. Candidate write_bytes differed (212,992 vs baseline 0), which
is filesystem/writeback accounting rather than a proven device-I/O improvement.
Lifetime maxRSS was 29,844–30,424 KiB including fixture setup; it is not a cleanup
interval peak. No memory reduction, cloud-store savings, whole-engine throughput
or production CPU percentage is claimed. Within-job per-operator scans remain.
All 12 owned command containers and four fixture directories were verified absent;
source restored, commands joined and queue released. Raw receipts and analysis:
`gates.json`, `analysis.json`, `source-inverse-proof.json`, `*-native-linkage-proof.json`,
`*-runtime-linkage.json`, `*-host.json` and `*-cleanup.json` under the evidence root.

## Integrated publication source

Main advanced by one STR-154 RocksDB-only commit during measurement. The ticket
was rebased onto `057c634a481ad96251e15bb497e8a179a9779778` as `8efe8319`.
Only the ledger prepend conflicted; both complete ticket entries were preserved.
Independent integration review approved and verified the three product/test file
hashes still match the measured candidate, while the entire incoming ledger is
preserved. Applicable instructions did not change.

The exact same normal dev fmt, affected locked check, strict Clippy, storage8,
cleanup4 (one opt-in benchmark ignored), and checkpoint18 tests all passed again
on integrated source, exit 0. Receipts: `rebased-gates/` under the evidence root.
Independent evidence review recommends KEEP with no findings. The optimized
measurement remains explicitly bound to its original base and pins; the unchanged
LocalFileSystem cleanup path does not exercise the incoming RocksDB write change.
No second optimized campaign was run. Publication/current-head CI are separate
from these completed source and correctness gates.

## Delivery boundary

This repair does not establish that STR-154's shared-write-buffer checkpoint
stall is fixed. Cleanup retry is not worker/process-loss recovery qualification.
Production deployment and live verification are separate steps; this task ends
at an independently reviewed PR with current-head CI passing, awaiting merge.
