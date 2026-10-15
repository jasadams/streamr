# STR-127 build pinning and native archive validation

Campaign 3 passed the scoped build, package, native correctness and worker
recovery acceptance at source `f46c641565c28a4c09446854f8a7c71d78f35ea7`.
Rebased source `71f248a3428850520c04c793221e5f705200e19f` subsequently passed
current-source dev/release packaging, UI, worker recovery and focused native
correctness checks. The original cold/source-native evidence remains bound to
its campaign source; current-source runs used warm caches.
No engine SQL, state, checkpoint, recovery or resource semantics changed.

## Source and evidence

Work started from fetched main `cd9520ee89f6ba1e4e9c7362b4b306fc257c78e1`
in `/home/jason/repos/streamr-str-127`, preserving the invoking dirty checkout.
Instructions read: `AGENTS.md`, `.claude/build-test.md`, `.claude/team.md`,
`docs/milestone-3-status.md`, `docs/milestone-3-work-plan.md`, and current STR-127.

Evidence root: `/home/jason/qa-evidence/str127-build-pinning-20261015/`.
Completed source-bound acceptance is in `campaign3/acceptance-complete.json`;
`campaign3/run-acceptance.py` and `runtime-check.py` are the exact supervisors.
All builds use maintained repository entrypoints and the shared queue. Build,
pin and dependent runtime checks retain one queue reservation. A single isolated
Cargo target and separate profile cache namespaces preserve active shared caches.

Each pin directory preserves frozen source/UI inventories, the compiled diff,
exact Cargo JSON and HTML timing report, image/toolchain/native archive receipts,
raw native build lock, CLI and host dependency fingerprint proofs, executable,
shared libraries, loader evidence, and owned-container/database cleanup results.
`source-manifest.json` links their hashes outside regenerable target caches.
Cold compiled-input diffs are empty; warm pins record the intentional harmless
comment appended to `crates/arroyo/src/main.rs`, then restore that source.

## Delivered behavior and review

`scripts/build-pin` defaults to dev; release requires explicit selection and a
purpose. It freezes inputs, including ignored local Cargo configuration and
caller-declared actual UI assets, runs maintained `cargo-dev`/`rust-build` with
an owned migrated PostgreSQL build database, and preserves a source-free package.
Opt-in API and OpenAPI packaging features embed development UI assets in both
runtime and host dependency contexts, leaving ordinary dev defaults unchanged.
The launcher uses the actual ELF with packaged `LD_LIBRARY_PATH`, preserving
`current_exe()` for worker subprocesses. Its compatible Bookworm Linux runtime
contract is recorded; invoking the packaged loader directly is avoided.

The image builds pinned upstream `librocksdb-sys` for dev O0/debug, release
runtime O3/debug, and release host O0/no debug. The selector uses actual Cargo
build-script settings, validates archive/source/dependency graph, features,
target, toolchain and flags, and refuses mismatches explicitly. Deliberate native
source compilation requires a recorded reason. A queued guard invalidates older
unwrapped RocksDB build scripts without deleting native outputs. Unrelated
feature-union additions remain eligible; archive-required dependencies retain
identity/checksum validation. `CC_ENABLE_DEBUG_OUTPUT` changes logging only.

Independent reviewer `/root/validation` issued written approvals after material
repairs, covering native selection, packaging, cleanup, feature symmetry and the
acceptance supervisors. No signatures or private keys were required. Campaign 3
native source/reuse and source-free runtime evidence was independently audited.

## Image and native identities

Campaign 3 build/runtime image:
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
Native build lock SHA-256:
`3f2ef48106fcb0287de914e119b4625fbb82e5ec0df4507f8483fa1cb090ec06`.
Exact toolchain, feature, ABI, compression and build-lock records are in each
pin's `image-native-receipt.json`.

| Archive | Actual native settings | SHA-256 |
| --- | --- | --- |
| dev | O0, debug true | `c1c76537d9d655fca93b9cc5e8f50f82406ef5da584294aee013605f69d5bd3e` |
| release runtime | O3, debug true | `cd8300d81598fb5e0e6b5dd73b55e1355fc004b7f4ca8b27341cf759ef927d54` |
| release host | O0, debug false | `313cba9749d60bae7ae74eef923f79c720f7542bb15cfac7d43280bb473b2b5b` |

Initial image construction exited 0; its 3625.081 seconds includes queue wait.
Cargo native stages reported 8m32s dev, 9m12s release runtime and 5m36s release
host. Later selector-layer rebuilds reused the native stage and preserved all
three hashes (`native-image-archive-reuse.json` in the evidence root). Image
construction is distinct from the following successful package/runtime campaign.

## Campaign 3 timings and native reuse

`cold-prerequisite.json` proves the initial target and profile cache namespaces
were empty. Dev starts with an empty target/cache. Release starts with a cold
release runtime/profile cache namespace while the single shared target retains
dev-warm host/build dependencies, including migration-helper artifacts. Release
is therefore not described as a wholly empty target. Both subsequent warm builds
include the declared source comment edit.

Times below are the recorded complete phase-wrapper seconds, including its
setup, build/pin or runtime work and finite cleanup. The Cargo-command column
comes from pin `build-result.json` and includes database migration, distinct from
the complete wrapper. All listed phase wrappers and their cleanup exited 0.

| Phase / result receipt | Complete wrapper seconds | Cargo command seconds |
| --- | --- | --- |
| `dev-cold-result.json` | 1110.087431641994 | 1071.1941016269848 |
| `dev-recovery-result.json` | 90.68998999404721 | — |
| `release-cold-result.json` | 2243.706355600967 | 2198.6719838629942 |
| `release-recovery-result.json` | 97.91190479509532 | — |
| `dev-source-edit-warm-result.json` | 68.91011598403566 | 39.99724557902664 |
| `release-source-edit-warm-result.json` | 123.37221157900058 | 84.14306437596679 |
| `deliberate-native-source-result.json` | 619.8306813039817 | — |
| `positive-native-source-audit-result.json` | 2.2772893080255017 | — |
| `return-to-prebuilt-result.json` | 18.488320705015212 | — |
| `actual-cargo-integration-result.json` | 17.07222452003043 | — |

All four package builds selected matching prebuilt archives and recorded no
RocksDB C++ compilation. Runtime and OpenAPI host linkage/profile proofs remain
separate, including release runtime O3 and host O0. Remaining Rust, codegen and
link work is visible in exact retained Cargo timing reports; other native
dependencies are not claimed compilation-free. These are build timings, not a
runtime benchmark or an estimate of performance savings.

| Pin | Executable SHA-256 |
| --- | --- |
| dev cold | `d40f8d4a5d2c658f856caceda9b01e68fc282c85a05c37a6cb47cd3f9588a2e4` |
| release cold | `ec4b374eaa46cae40973375ba98364966477c8c03874f6ebdcd654760e1c4326` |
| dev source-edit warm | `8ad87c23695972f75be33e3d9c4f155d1a11ffd7cc23ead109c5f8d61d72c8a9` |
| release source-edit warm | `22c7449c7d6510b0317804a35bd203f4ee7f80ecdf032debee606eaf8186004e` |

The two warm compiled diffs share SHA-256
`0580fcb5a5572110f1ea4c809d4cf452daf66facd199a5d8698265f15de90b0f`.
Cold and warm pins preserve actual source bytes; HEAD alone is not used to hide
the intentional warm edit.

## Source-free UI and real-worker recovery

Both cold packages ran in the exact image with network disabled and mounts only
for package, harness, receipts, cases and the pinned Python oracle runtime. No
source checkout was mounted. Shared libraries came from `/artifact/lib`; worker
identity was the real `/artifact/bin/arroyo` ELF.

Both recovery cases observed checkpoint epoch 1, faulted generation 1 after 150
observed raw rows, and observed a distinct generation 2 worker. Both finished
with exact raw rows 0 through 999 and exact aggregate count 1000, sum 499500,
minimum 0 and maximum 999. The dev case took 80.46090779395308 seconds internally;
release took 80.34296817693394. Full rows, committed checkpoint outputs, fault,
replacement identities and results are retained under each profile's
`*-runtime-cases/recovery/`. Both raw-output hashes are
`0e66723695e9143400e3af6f11ba94efe57af4da9c87fb73e8b614d49ce35d91`;
aggregate transition counts differ (73 dev, 74 release), with identical exact
final aggregate values.

Both UI HTTP checks returned 200 with 4728 bytes and matched the declared served
asset hash `85f3d429f708bc297d19f891259a1ed2a3b1ec3feebf7a693b271425e5281712`.
These are actual source-free HTTP checks, beyond the package CLI help smoke.

Both runtime result receipts record job state `Finished`, cleanup complete,
empty cleanup errors, and actual top-level CLI exit **-9** from finite forced
cleanup after the value checks succeeded. A graceful CLI exit 0 is not claimed.
Recovery supervisors and container removal exited 0. Owned build databases,
anonymous PostgreSQL volumes, helper containers and runtime containers were
removed; named Cargo caches were preserved. Exact cleanup receipts remain with
the pins and campaign, including the release database volume audit.

## Positive source-native mode and return

The separate deliberate comparison used `ROCKSDB_COMPILE=1`,
`CC_ENABLE_DEBUG_OUTPUT=1`, and reason
`STR-127 deliberate source-native correctness comparison`, running:

```sh
scripts/cargo-dev test --locked -p arroyo-state --lib \
  live::rocks::tests::rocks_conforms_and_reopens_explicitly -- --exact --nocapture
```

The original upstream build script performed **328 observed RocksDB C++ compile
commands**, including `-O0`, `-gdwarf-4`, `-fPIC`, `-std=c++17` and `-DLZ4=1`.
`source-native-positive-proof/receipt.json` retains the exact commands, selector
reason, target/settings, output/stderr hashes and archive size 905773154 bytes.
Resulting source archive SHA-256:
`de82cb1796ddad109a8ee4adf51ffd8a99fb7a9a54f4ec5fca25995f248f453b`.
The exact conformance/reopen test passed (1 passed, 0 failed), covering values,
atomic batches, snapshots, pagination and persisted reopen. This is actual native
source compilation and correctness evidence, separate from image construction.

The same exact test passed again after returning to prebuilt mode. Its preserved
proof records selection of the dev image archive without RocksDB C++ compilation.
The actual-Cargo integration fixture also exited 0, covering old-cache
invalidation, dev/release/host settings, source switching and explicit mismatch
handling. That fixture is selector integration evidence, not a substitute for
the real native correctness test.

## Checks and preserved failed attempts

At validated source, affected script checks exited 0: cargo-dev 21 tests,
native selection 25 tests, build-pin 17 tests, preflight Clippy wrapper six tests,
ticket guard 48 checks, shell syntax and whitespace checks. Independent review
and these checks supplement the completed native/package/runtime evidence.

Earlier failures are retained in the evidence root and `campaign2/`:

- The first isolated dev build rejected the redundant upstream `lz4-sys` feature
  activation alongside `lz4`. The repair recognizes the existing pinned feature
  closure; it does not add a compression feature or substitute an archive.
- Campaign 2 selected the correct dev archive but failed compiling the OpenAPI
  host API: the shared rust-embed derive macro enabled `debug-embed`, while its
  host library did not. Optional packaging feature forwarding aligns both host
  and runtime libraries. Ordinary dev defaults remain unchanged.
- The first repaired warm CLI compiled, but the frozen-input guard caught a
  bytecode cache created by the host preflight import. The helper now disables
  bytecode writes before importing; an isolated subprocess regression preserves
  the strict inventory check. That failed packaging attempt remains recorded.
- A previous build database left an anonymous volume. It was removed only after
  confirming zero container references; maintained owned-database cleanup now
  removes its anonymous volumes while preserving named Cargo caches.

Failed attempts remain failed. Their logs, exact exits and cleanup audits were
not replaced by the successful campaign. The repaired warm diagnostic was not
relabeled cold; campaign 3 supplied fresh cold prerequisites.

## Completed current-source verification and remaining delivery

The branch was rebased onto main `277adcb8` at source
`71f248a3428850520c04c793221e5f705200e19f`. Applicable guidance and dependency
pins were reconciled and unchanged. Main was refetched and remained `277adcb8`.
`/home/jason/qa-evidence/str127-build-pinning-20261015/current-main/current-source-complete.json`
records the subsequent completed checks at that exact rebased source, using the
same immutable image and native archive identities recorded above.

Both profiles produced packages with empty compiled-input diffs. Preserved
native mappings prove dev O0/debug, release runtime O3/debug, and release host
O0/no debug selected the exact matching image archives, with no RocksDB C++
compilation. These runs reused the shared target and caches; **no current-source
cold-build claim** is made. Campaign 3 retains the earlier source-bound cold,
source-edit reuse and genuine source-native compilation evidence at `f46c6415`.

| Current-source phase / result receipt | Complete wrapper seconds | Cargo command seconds |
| --- | --- | --- |
| `dev-rebased-current-result.json` | 171.12880757497624 | 134.0609471789794 |
| `dev-recovery-result.json` | 90.86627445102204 | — |
| `release-rebased-current-result.json` | 452.6962609270122 | 413.7088528579334 |
| `release-recovery-result.json` | 97.44305664300919 | — |
| `current-prebuilt-native-result.json` | 15.238503213971853 | — |

All five current-source phase wrappers and container cleanup commands exited 0.
The retained package executable hashes are:

| Current-source package | Executable SHA-256 |
| --- | --- |
| dev | `303d9412bd98dee7a0c2341fb35cbc0037e43a92d730ad3011f0d73fdd54f76f` |
| release | `72bb3abea127464b28c4e517b36c7ac9419916745415c8548cc4178a0f3514d8` |

Both packages again passed source-free real-worker recovery: checkpoint epoch 1,
generation 1 loss and generation 2 replacement, exact raw rows 0 through 999,
and exact final count 1000, sum 499500, minimum 0 and maximum 999. Both UI HTTP
checks returned 200/4728 bytes with the same exact served-asset SHA-256 recorded
above. Full results and identities are under `current-main/*-runtime-cases/`.
Each runtime result retains actual CLI exit -9 from finite post-success cleanup,
job state `Finished`, cleanup complete and no cleanup errors; supervisor exit 0
does not substitute for a graceful CLI exit. No checkout was mounted.

The exact prebuilt-mode test
`live::rocks::tests::rocks_conforms_and_reopens_explicitly` also passed at the
rebased source (1 passed, 0 failed), separate from the earlier positive
source-compilation comparison. Its command, native selection, test output,
wrapper timing and cleanup are preserved in `current-prebuilt-native-result.json`
and the corresponding log.

Final independent review approved the implementation, rebased source and
completed evidence/report with no remaining acceptance gap. Current-head PR CI
is tracked separately on the pull request. No merge, release, deployment,
broader engine qualification or runtime performance speedup is claimed here.
