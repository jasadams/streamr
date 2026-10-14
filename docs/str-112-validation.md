# STR-112 validation

The retained candidate removes a discarded full key encoding from `decode_key`:
`encoded_key_size` performs the same final ownership/routing/length validation
without allocating another namespace/key buffer. No encoded bytes, parser checks,
public interface, admission, snapshot lifetime or checkpoint protocol change.
The focused decoder CPU measurement improves in both alternating pairs; the
whole-export diagnostic is inconclusive. **The strong native campaign is closed
failed/inconclusive: only one matched pair qualified, and its results were mixed.
STR-112 acceptance remains open; no stable native throughput improvement or ticket
completion is established.**

## Source and instructions

Frozen B: `6793409cb53eb7b13f5a591ce1717bcf1e846de8`, branch
`jason/str-112-export-20261014`. B has current telemetry/native iterator upper
bounds; the sibling first-entry scan fix is absent. No rebase or sibling
codec/calendar/SQL change was included in these comparisons. Shared encoding
ownership was coordinated after STR-114 entered review.

Read the worktree `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md` and work plan, current `STR112-task.json`,
`/home/jason/.claude/agents/feature-implementation-engineer.md` and canonical
`/home/jason/repos/kyomi-private/skills/backlog-fast/SKILL.md`. `CLAUDE.md` and
`docs/CODING_STANDARDS.md` are absent at B. Current user policy supersedes old
signing and automatic ticket time-limit rules. Task JSON is orchestration-only
and is not staged.

Candidate source hashes:

- encoding.rs: `071011ccd06e57a25c5422306db184a7cd9435f5ee7e3d0d482a063cee47e1be`.
- checkpoint.rs: `c009f61c7be597fc3a233faa4e7fd9288421de402d65c29442c4799d62fef1ae`.
  Its production implementation is B; its new regression uses the full configured
  native restore entry point during concurrent post-snapshot writes.
- rocks.rs remains exact B: `741627563d47e296ba24beec5b54c10577ecb3750279ca19e1e8d65ce9c4e874`.

Evidence root: `/home/jason/qa-evidence/str112-export-20261014` (outside Cargo
cache). Every build/capacity run used the shared foreground queue and completed
with its actual exit retained; no background build, cap increase or new pool.

## Current checks and decoder CPU measurement

`encoding-pairs/commands.json` records all current candidate checks, each exit 0:

- `scripts/cargo-dev test --locked -p arroyo-state --lib live::encoding::tests --no-fail-fast`: 2 passed.
- Same command with `live::checkpoint::tests`: 17 passed.
- Same command with `live::rocks::tests`: 14 passed.
- `scripts/cargo-dev check --locked -p arroyo-state`: exit 0.
- `scripts/preflight-clippy.sh -p arroyo-state`: exit 0, CI flags plus locked,
  strict all-features/all-targets checks.
- `scripts/cargo-dev fmt --all -- --check`: exit 0.

Decoder regressions cover every ownership, empty/zero-containing keys and
identifiers, routing endpoints/singletons, every truncation of valid cases,
unsupported version/tag, trailing bytes and malformed partition/range/hash/
escape cases. The configured native child uses the same 62 MiB pool, 2 MiB scan
limit and two blocking operations. It captures 64 incompressible values, exports
concurrently with updates/deletion/insertion, restores a fresh native owner and
asserts all original values and excluded post-barrier keys. Its owned child has
a 60-second kill/wait bound and does not mutate parallel tests' global config.
Existing cancellation, leftovers/retry, immutable-file cleanup and tight-budget
checkpoint regressions passed in the same focused suite. This is native state
recovery evidence, not full operator qualification.

`decoder-cpu/` contains the single bounded CPU measurement, two actual builds,
external pins, raw logs and receipts. Both state compiler artifacts explicitly
report `fresh=false`; dependency artifact/native archive identities match.
The state crate used explicit O3 in both arms; dependency crates retain their
actual Cargo profiles. Exact 4096 export-pilot keys and 1024 passes were fixed
before outcomes (4,194,304 decodes per run). Typed values are fully asserted
before/after timing; std `black_box` retains each result. `/proc/self/stat` measures
this process's user/system CPU ticks, at 100 Hz; no I/O/native work is inside the
loop. All builds and four runs exited 0.

| Order | B wall ms | C wall ms | B CPU seconds | C CPU seconds |
| --- | ---: | ---: | ---: | ---: |
| B then C | 724.383 | 290.358 | 0.72 | 0.29 |
| C then B | 728.749 | 301.524 | 0.73 | 0.30 |

This is about 59–60% less decoder CPU (173 ns versus 69–72 ns per decode), with
10 ms CPU tick resolution and two samples per arm. It is not native processing
or durable throughput and cannot predict whole-export speedup.

## Whole-export diagnostic: inconclusive

`encoding-pairs/` preserves two alternating pairs of the exact 4096-key exporter,
2048/8192-byte incompressible values, 62 MiB pool, 2 MiB scan limit, two blocking
operations and identical exporter/native instrumentation. Only decoder validation
differs. Both O3 state builds were actually compiled/pinned in the same queue
reservation (`fresh=false`). Native archive, SHA2/Parquet/RocksDB Rust dependency
artifacts and immutable image match; those dependency crates use Cargo O0, while
native C++ uses the image's pinned prebuilt archive. This is not the optimized
production profile. Every run exited 0 with full serialized key/value assertions,
including pre-snapshot values after live replacement/deletion.

| Payload | Order | B export ms | C export ms | B scan/admission ms | C scan/admission ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 2048 | B then C | 764.735 | 736.383 | 151.199 | 146.600 |
| 2048 | C then B | 714.377 | 798.285 | 144.816 | 165.419 |
| 8192 | B then C | 3309.543 | 3188.827 | 707.344 | 666.977 |
| 8192 | C then B | 2938.452 | 3097.793 | 624.231 | 706.601 |

Rows/groups/files/bytes match: 4096 rows, 456 groups / 35 files / 8536792 bytes
for 2048; 2048 groups / 137 files / 34100478 bytes for 8192. Unrelated SHA/Arrow
phases vary too. The first pair improves while the reversed pair regresses;
there is no stable whole-export improvement. `summary.json` excludes standalone
empty probes from exporter native scan attribution.

## Prior rejected targets and restore emptiness

Full enumeration remains necessary: this protocol writes a complete immutable
logical snapshot of unknown keys, retaining unchanged rows and excluding
post-barrier writes. It cannot substitute point lookups or incremental/SST
manifests. STR-25 remains separate architecture.

The initial dev pilot (`pilot-baseline-2.log`) exited 0 and measured native
iterator/seek restarts per page, scan/admission, Arrow/key construction,
write/flush and footer/checksum/upload. Native deleted/internal skips were captured
inside the actual blocking scan. Exact-size BinaryBuilder key materialization
regressed all four materialization comparisons; reserved page-capacity
materialization also regressed and changed file counts (38/147 versus 35/137).
Both were removed. Raw sources/logs and `diagnostic-results.json` remain preserved.

The third candidate moved synchronous Parquet work onto existing bounded
`run_blocking`. Its cancellation/restore checks passed but every paired export
regressed. It was rejected completely, including its CPU helper and specific
cancellation test. The meaningful configured snapshot regression was retained.
Patch/source receipts are `rejected-scheduling-candidate.patch` and
`blocking-pairs/production-checkpoint.rs`; all four raw comparisons remain:

| Payload | Order | B export ms | C export ms | B scheduling ms | C scheduling ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 2048 | B then C | 766.393 | 797.435 | 115.345 | 158.129 |
| 2048 | C then B | 758.997 | 790.357 | 118.292 | 156.242 |
| 8192 | B then C | 3118.129 | 3391.979 | 471.223 | 669.045 |
| 8192 | C then B | 3119.993 | 3188.052 | 487.787 | 634.372 |

All four runs passed content assertions and preserved original groups/files/bytes;
the bounded handoffs added overhead. No production paired acceptance was run for
that rejected implementation.

Failed gate source `9639cc69…` is archived in `state-gate-source.json`: checkpoint
exit 101 (17 passed; unconfigured native fixture requested 4194916 scan bytes
against 2097152), Rocks/check/strict-Clippy/fmt exit 0. The narrowed private-restore
helper workaround was cancelled before compilation (owned waiting job only,
actual exit 143) and removed. Correct configured-child source `3945a926…` passed
all 18 checkpoint tests, strict Clippy and fmt; its tests belong to the rejected
scheduling candidate, not the current source. Both complete source hashes and
raw failure/results remain in their receipts.

The namespace-empty preflight is measured separately and unchanged. It requests
one entry using a stable snapshot; native upper bounds prevent traversal of other
namespaces but do not skip deleted history inside this one. Fresh capture/scan
was about 0.29/0.40 ms; nonempty 0.39/0.70 ms. With 4096 tombstones and an old
snapshot held, capture/scan was 18.96/74.80 ms, with 4096 deleted and 4097 internal
skips. Dropping that old view without compaction was 18.34/75.64 ms. These dev
native-history probes are not full fresh-worker recovery; no safety check was
weakened or emptiness optimization claimed. Iterator retention was not implemented
because native allocations across async uploads lacked an existing bound proof.

## Optimized native baseline and remaining acceptance

Production B build/pin exited 0 (1648.889 s) under the existing reviewed supervisor;
private build database cleanup exited 0. CLI SHA:
`4a78032dc0baef8701c946d9b6c85348cd0e70646cc42d8c3c0d975dd82c08f1`.
Manifest SHA: `25debba5fd6d37b795f2ba41f4dd443872c1a0307649a9a91edf9e6b16b11576`.
Normal CLI dependency fingerprints prove state/SHA2/Parquet/RocksDB Rust and
native C++ use O3; host-only OpenAPI O0 artifacts are distinct. Correct mapping
receipt: `production-dependency-profile/normal-cli-dependency-profiles.json`.

Reviewed fresh clone/catalog/prefix receipts reuse epoch 87, source counter
653200, clock 1791799217.297272888, 400000 keys / 2048-byte payload / 62 MiB pools.
Saved cadence is 120 seconds despite the global 2-second default. The reviewed
bounded native attribution launcher completed actual exit 0, runtime exit 0,
`qualification=false`, with all 14 callbacks and independent readiness/prefix
approval. Restore readiness took 318.243 s. Continuous owned-session RSS sampled
516530176 bytes (492.602 MiB), 5576 samples, maximum gap 0.170 s, no sampler errors;
it stayed below the unchanged 512 MiB cap but is not an exact maximum.

Steady capture: 2345 samples, 34.669-second metrics bracket, source/table receive
7100 and other three owner receive deltas 7094. Epoch 88 capture: 4253 samples,
33.179-second bracket, source/owner counters unchanged, 111.02 CPU seconds. These
are emissions/receives, not durable progress. Leaf report resolves SHA256 0.85%,
LZ4 decompression 5.15%, LZ4 compression 7.24%; unresolved/incomplete callchains
prevent full stage attribution. The earlier dev SHA dominance is not a production
claim. Engine/profilers/container were removed/reaped successfully within the
reviewed shared 1140-second main / 60-second cleanup bound; no owned process
remained. Evidence: `production-profile/profiling-attempt` and `offline`.

The coordinator committed the independently reviewed candidate as
`29e98868229733385c334f7c8713c5927c8199aa`, retaining frozen B as its parent.
The optimized C build/pin completed exit 0 in 1721.051 s; its private build
PostgreSQL container removal also exited 0. C binary SHA256:
`e3e06037736fb6b9ec45ac3ff9e22cf854ff732b8f61bbfb820140fe448b7102`;
manifest SHA256 `88a649763843ee42ffbc7238413cf4fffe1e10e70222eda74fee69318e51e9dc`.
Normal CLI state/SHA2/Parquet/RocksDB Rust libraries are O3, as in B. Both arms
have 605 compiled source files; only checkpoint.rs and encoding.rs differ.
Native O3 RocksDB rlib, source archive and build output hashes match exactly.
Receipts: `production-candidate`, `candidate-dependency-profile` and
`production-comparison/pin-comparison.json`. This is build/source evidence, not
native performance acceptance.

Independent review approved the C pin, adapted driver and all four immutable
arm inputs. Four clone preparations completed exit 0, each verifying 116094 files
and 21773551294 logical bytes against the same seed with distinct inodes; all four
inventory/hash/size records match. `production-comparison/clone-preparation-result.json`
and per-arm review receipts retain the exact results. Final preparation manifests
were frozen with both readiness flags true and approved for execution. The first
strong attempt exposed a preservation harness error, recorded below. Independent
review approved the Path repair, fresh B1 seed, reused untouched other seeds and
renewed material manifests before the complete fixed-order campaign.

The repaired foreground reservation ran B1/C1/C2/B2 once with unchanged settings:
four owners, 400000 keys, 2048-byte payload, 62 MiB pools, 512 MiB owned RSS,
exact internal clock/prefix and 120-second cadence. Native main/cleanup bounds
were 2340/60 seconds per arm; post-stop offline audit had a separate 900-second
bound. No cap increase, checkpoint substitution or failed-arm retry occurred.


## Strong attempt: preservation harness failure

The first B1 arm passed immutable input/readiness gates, observed all 14 native
callbacks and Running generation 10 after 344.675 s, and observed fresh checkpoint
88 complete. Its API total checkpoint span was 131.955 s (operators 131.752 s).
Preservation then failed with `AttributeError: str has no attribute is_symlink`:
`shutil.copytree` supplied string paths to `reflink_file`. The first checkpoint was
not accepted; no durable-rate or owner-count result is claimed. Actual B1 runtime
and launcher exits were 2, with `comparison_eligible=false`.

The continuous owned-session RSS sampler recorded 467718144 bytes (446.05 MiB),
6229 samples, maximum gap 0.116 s and no errors. There were 278 metric captures.
Cleanup completed without errors, the container was absent, no owned native
process remained, the caller joined within the shared deadline, and frozen
provenance remained unchanged. Source/owner emission/receive metrics are retained;
they are not durable source-state counters.

C1 was interrupted only during its untimed prelaunch hash guard, before any native
attempt directory or engine launch; C2/B2 were not dispatched. The owned wrapper
was stopped before it could dispatch another arm, C1's interruption was verified,
and the wrapper then exited 143. These remaining native scenarios are unverified,
not failures of the engine. Exact identities/actions/exits are retained in
`failed-copytree` and `known-harness-failure-interruption.json`; the failed B1 arm,
including its mutated clone and incomplete preservation directory, is retained
intact at `production-comparison/failed-copytree/B1`.

The only harness repair normalizes both copy arguments to `Path`. An actual
same-filesystem `shutil.copytree` regression passed with string arguments,
nested/empty files, exact contents, distinct inodes and symlink rejection
(`copytree-regression-result.json`). Rust source, pins, settings, selection and
finite bounds are unchanged. Fresh B1 preparation exited 0; untouched C1/C2/B2
clones were reused with mandatory prelaunch guards. The renewed fixed-order
campaign below completed after independent review. The failed original interval
and its interrupted/unexecuted scenarios remain part of the evidence.

## Final strong native outcome: failed/inconclusive

Foreground session `69554` exited **2**. B1, C1 and C2 passed readiness, counter,
phase, first-three selection, RSS, cleanup and post-stop structural audit gates.
B2 exceeded the unchanged 512 MiB RSS gate after checkpoint 89, before 90.
All four arms were collected; the campaign is closed, with two matched pairs
unavailable and STR-112 acceptance open.

| Arm | Readiness s | CP88/89/90 persisted counters | CP88/89/90 API spans s | Durable CP88→90 events/s (bounds) | Sampled RSS MiB | Exit |
|---|---:|---|---|---|---:|---:|
| B1 | 294.049 | 703100/756600/822600 | 140.488/128.970/145.933 | 231.968 (231.841–232.085) | 498.816 | 0 |
| C1 | 353.225 | 729200/800300/852200 | 141.629/146.852/135.447 | 235.354 (235.237–235.480) | 460.758 | 0 |
| C2 | 287.841 | 687900/741700/794600 | 145.078/163.920/139.623 | 196.211 (196.114–196.305) | 483.301 | 0 |
| B2 | 357.567 | 693700/764300/uncompleted | 121.718/142.746/uncompleted | ineligible | 513.215 | 2 |

Durable progress uses persisted source-state deltas between observed CP88 and
CP90 completion brackets. Initial epoch 87 counter 653200 and CP87→88 carry-in
are reported separately and excluded from that numerator. Source emissions and
owner receives below use eligible post-warmup steady intervals, weighted by actual
observation time; they are not durable counters.

| Arm | Source / table receive events/s | Aggregate receive | Calendar receive | Session receive | Whole metric-window native CPU s |
|---|---:|---:|---:|---:|---:|
| B1 | 334.578 | 335.094 | 335.087 | 335.094 | 2099.50 |
| C1 | 322.053 | 321.956 | 322.125 | 321.903 | 2303.92 |
| C2 | 234.202 | 234.039 | 234.045 | 234.039 | 2173.73 |
| B2 (censored) | 333.682 | 333.610 | 333.610 | 333.737 | 1674.05 |

C1/B1 durable rate changed **+1.46%**, while its two individual durable intervals
changed **+24.05% / −18.20%**. Weighted steady source/table rate changed **−3.74%**;
aggregate/calendar/session receive rates changed **−3.92% / −3.87% / −3.94%**.
This small mixed first pair does not establish stable improvement. C2/B2 is
ineligible: B2 has no CP90 and no full offline count proof. Its censored processing
metrics must not substitute for a qualified second pair or support a pooled claim.

B2 sampled 538144768 bytes (513.214844 MiB), exceeding the cap by 1273856 bytes
(1.214844 MiB). This is a demonstrated failure of this baseline arm under the
required resource bound, not proof that every such run is impossible and not
permission to alter limits or behavior. RSS sampling had no errors in any arm;
maximum sample gaps were 0.113/0.120/0.157/0.125 seconds respectively. Sampled
peaks are not exact maxima. All arms completed cleanup with no errors, containers
absent, no owned native process, callers joined within the shared deadline and
unchanged frozen provenance. The failed B2 arm was retained without retry.

The three eligible arms' nine post-stop audits exited 0. Their proof scope is
checkpoint protocol/namespace/schema/marker/path/checksum closure, source cursor
and clock, sink offsets, and complete Parquet record/count enumeration. This is
not fresh-worker full-value/operator-recovery qualification, a full application
oracle or combined STR-103/126 qualification. The separate configured native
snapshot regression above establishes its exact 64-row fresh-backend values only.
B2's failed-arm protocol did not run heavy offline audits.

Evidence root: `/home/jason/qa-evidence/str112-export-20261014/production-comparison`.
`paired-outcome.json` (SHA256
`d63c61f77958c9565bfbeb4bcc6df484fa3fb6b5ff2249ed93e3abe720261f6d`)
contains all arm exits, exact completion brackets/deltas, preservation durations,
steady interval counts/durations, CPU windows, raw backend histogram sum/count
endpoint deltas, native log compaction receipts, RSS, cleanup and audit hashes.
`owner-phases.csv` records all 44 completed owner Sync/Async API spans and sizes;
Async includes export plus metadata, and Sync includes preparation/flush/capture.
`native-counts.csv` records all 36 audited owner/epoch rows, row groups, files,
object bytes and key/value bytes. Its page counts are explicit greedy
reconstructions from preserved ordered rows and unchanged page bounds; they are
not measured native scan pages or deleted/internal skips.

Whole native CPU windows include processing, checkpointing and background
compaction, have different lengths, and B2 is censored; they are not exporter-only
or per-owner CPU comparisons. Existing backend histogram durations include
operations/admission waits over those metric windows, not pure CPU. Compaction
CPU covers fresh database lifetime including restore and warmup, not just export.
Exact source paths/endpoints and these scopes remain in the analysis artifact.

The retained increment removes measured decoder allocation/encoding cost, with
59–60% less isolated decoder CPU in both alternating pairs. Whole-export results
remain inconclusive and this native campaign failed/inconclusive. Independent
review approved publication of the narrow increment with these limits; STR-112
acceptance remains open. Integration/current-source gates, publication/CI and
remaining recovery/qualification evidence are coordinator-owned, not claimed here.

## Delivery integration checks

The narrow increment rebased cleanly onto main `196a9f72`; benchmark source and
pins remain frozen at their recorded B/C commits. At delivery source `9c675052`,
one foreground queue reservation completed exit 0: full `live::` state tests
(68 parent tests plus the configured child, no failures), locked state check,
strict all-features/all-targets Clippy and container formatting. Test/check state
compiler artifacts report `fresh=false`, with exact source hashes and unchanged
start/end HEAD. Receipts: `integration-gates/commands.json`, SHA256
`ba3d7e15376d5ee06e2a808e64320477a88ac97daf8d8b879a8d63301853b386`.
These establish current-source focused gates, not integrated performance or full
operator recovery qualification. Final documentation corrections only relabel
the 64-row restore as fresh-backend and record these gates.
