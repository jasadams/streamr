# Arroyo Fork Build & Test Procedures

Known build and QA infrastructure failures, reproductions and verified repairs
are recorded in [docs/build-qa-troubleshooting.md](../docs/build-qa-troubleshooting.md).
Consult it before repeating a settled investigation, and add new findings with
their source/image versions, evidence and unresolved checks.

## Build Environment

The maintained repository scripts are the default build interface. Use
`scripts/cargo-dev`, `scripts/preflight-clippy.sh` and the shared
`scripts/rust-build` queue; do not choose an external one-off builder simply to
obtain source hashes, executable pins or packaging.

Use normal dev/test builds for correctness, resource and recovery validation so
the matching precompiled RocksDB archive is reused. Release is an explicit choice
for optimized measurements or release artifacts. Record the purpose, selected
profile and native archive/source mode before expensive work. If build or pinning
support is missing, improve the maintained scripts within the authorized scope.
`scripts/build-pin` supplies the same packaging and provenance path for both
profiles. The image supplies matching dev, release runtime and release build-
dependency native archives.

Arroyo requires Debian Bookworm toolchain. Fedora 44's GCC 16 and OpenSSL 3.5 are incompatible
with vendored C dependencies (sasl2-sys, rdkafka-sys, aws-lc-sys).

**All builds must use the dev container:**

The image installs rustfmt and Clippy. Its `streamr-rustc-cache` wrapper caches
Rust compilation only. Do not replace it with `RUSTC_WRAPPER=sccache`: cc-rs
would also wrap C compiler probes, and a cached OpenSSL assembler probe writing
to `/dev/null` fails with EBUSY and generates incompatible Solaris assembly.

```bash
# Build the dev container (one-time)
scripts/cargo-dev --build

# Run cargo commands inside the container
scripts/cargo-dev check -p arroyo-worker
scripts/cargo-dev test -p arroyo-worker
scripts/cargo-dev clippy -p arroyo-worker -- -D warnings
```

## Build acceleration

The amd64 development image pins Rust 1.96.0 and sccache 0.14.0 (verified
against its release checksum), uses mold for linking, and persists up to 30 GiB
of compiled Rust artifacts in the `streamr-sccache` named volume. Cargo download
volumes and `target/milestone2-runtime` remain in use. Rebuild the image with
`scripts/cargo-dev --build` before using these changes; changing the compiler
and linker flags requires an initial rebuild of existing artifacts.

Dev/test profiles use line-table debug information and enable incremental
compilation for repeated edits to workspace crates. Unchanged dependencies remain
eligible for sccache; incremental Rust compilations run through rustc instead.
The wrapper supplies these profile defaults even with an existing dev image.
Full debugger type/variable information is reduced. The wrapper stops the cache
server before its disposable container exits so pending writes finish while
preserving Cargo's exit status.
`scripts/cargo-dev --stats` reports persisted cache size; request/hit counters
are per container and reset on each run. Use Cargo's `--timings` to measure
build performance. The machine-wide build queue and four-job default remain.

The development image contains pinned RocksDB 10.4.2 static archives built once
through the original `librocksdb-sys` build script with LZ4. The selector uses
Cargo's actual native optimization, debug, target and feature settings for each
invocation: dev O0/debug, release runtime O3/debug, and release build dependencies
O0/no debug. It validates source, dependent-native, toolchain, compiler flags and
archive receipts under `/opt/streamr-rocksdb`. These image archives survive Cargo
cache eviction and ordinary Rust/source edits.

Rebuild with `scripts/cargo-dev --build` when the native pin changes. Old or
mismatched images fail before a build starts. Queued startup invalidates only
stale unwrapped RocksDB build scripts from older images; it preserves native
outputs and other crate artifacts. The wrapper executes the original upstream
build script after selecting its archive and supplies Linux C++ linkage.

Custom profiles/targets, compiler overrides, local `.cargo/config` and Cargo
`--config` require an explicit native source reason. For deliberate comparison:

```bash
ROCKSDB_COMPILE=1 STREAMR_ROCKSDB_SOURCE_REASON='native source comparison' \
  scripts/cargo-dev build --locked -p arroyo-worker --lib --timings
```

Unsupported actual native settings fail explicitly instead of substituting a
wrong archive. Keep native mode stable during ordinary iterations: switching
modes invalidates RocksDB and its dependents.

## Run preserved test executables and fixtures

For finite validation outside Cargo caches, set `STREAMR_BUILD_EVIDENCE_ROOT`
to an existing canonical absolute evidence directory, then use
`scripts/cargo-dev --exec <command> ...`. The wrapper mounts that directory at
the same absolute path in the maintained image, preserving caller fixture and
pinned-executable paths. Paths containing colon/control characters, aliases and
`/app` mounts are rejected. Use one outer `scripts/rust-build` reservation for
the build, executable preservation and dependent validation. This optional mount
does not change profiles, native archive selection or default container mounts.

`STREAMR_BUILD_CONTAINER_NAME=streamr-build-pin-...` declares an external owner.
These named containers remain after exit so the owner can capture their actual
container ID, including for short commands. The owner must remove its container
before releasing the queue reservation. Ordinary unnamed commands use `--rm`.

## Build and preserve a CLI package

Use a new evidence directory outside checkouts and Cargo caches, and supply
actual built UI assets (`webui/dist`) rather than placeholders:

```bash
scripts/build-pin --root /absolute/evidence/dev-pin \
  --purpose 'focused correctness and recovery' --ui-assets /absolute/webui/dist
scripts/build-pin --root /absolute/evidence/release-pin --profile release \
  --purpose 'optimized measurement executable' --ui-assets /absolute/webui/dist
```

The entrypoint reserves the shared queue, freezes declared tracked/untracked
inputs, starts and migrates an owned disposable build database, and invokes
`scripts/cargo-dev`. It enables `arroyo-api/embedded-ui` so dev packages also
embed the supplied UI. It records actual Cargo profile and host/runtime native
linkage, source/image/archive/executable hashes, build timing/exits, and cleanup.
It preserves the executable, dynamic libraries and receipts outside Cargo's
regenerable caches. `source-manifest.json` is build evidence; successful packaging
and CLI startup do not establish worker recovery or performance qualification.

Use `package/bin/run-arroyo` in the recorded compatible Debian Bookworm runtime
image. The launcher preserves the executable identity required by process-worker
scheduling. The package includes its native libraries and loader receipt; it is
not a cross-platform executable.

`--native-source-reason 'concrete reason'` selects deliberate source compilation
through the same entrypoint. `--expected-source COMMIT` checks the invoking HEAD;
exact dirty and copied inputs remain separately hashed. `--database-image` and
`--image` are resolved to immutable local image IDs before execution.

Normal builds retain the canonical shared target and cache volumes. For isolated
cold-cache acceptance only, `STREAMR_SHARED_TARGET_DIR=/absolute/isolated-target`
and a unique `STREAMR_CACHE_NAMESPACE=streamr-cold-validation` isolate target,
registry/git and compiler caches without deleting active shared caches. Serialize
these checks through the same queue; reuse the single isolated target sequentially
and preserve receipts before removing owned validation caches.

Linked Git worktrees automatically mount the original checkout's `target/` at
`/app/target`, using `target/milestone2-runtime` for every queued build. Do not
add another mount for `/app/target`. The queue still serializes builds; alternating
source revisions can invalidate workspace incremental work, while the prebuilt
native library remains available independently of Cargo cache eviction.

To compare against the previous non-incremental mode, run
`CARGO_INCREMENTAL=0 scripts/cargo-dev build --locked -p arroyo-worker --lib --timings`.
Explicit `CARGO_INCREMENTAL` values are forwarded to the container; this global
override also affects release builds. Without that override, release profiles
are unchanged. `CARGO_PROFILE_DEV_INCREMENTAL=false` and
`CARGO_PROFILE_TEST_INCREMENTAL=false` disable only the respective profile.
Keep the selected mode stable during normal development. Warm each mode before
timing the same source edit, excluding the initial build and queue wait.

## Crates that build natively on Fedora 44

These crates have no vendored C dependencies and can be checked locally:
- `scripts/rust-build cargo check -p arroyo-rpc` (proto generation)
- `scripts/rust-build cargo check -p arroyo-datastream`
- `scripts/rust-build cargo check -p arroyo-operator`

## Crates that require the dev container

These build native dependencies, including Kafka/SASL or RocksDB:
- `arroyo-worker`
- `arroyo-planner`
- `arroyo-connectors`
- `arroyo-state` (RocksDB, LZ4 and runtime libclang bindings)
- `arroyo` (top-level binary)

## Quick check (per modified crate)

```bash
scripts/cargo-dev check -p <crate>
scripts/cargo-dev clippy -p <crate> -- -D warnings
```

## Test commands

```bash
scripts/cargo-dev test -p <crate>
```

## Notes
- Reuse `target/milestone2-runtime` for container builds, including `podman exec`
  commands, and use the dev/test profile defaults above. Avoid creating
  a second default `target/debug` cache or separate caches per ticket. Serialize
  builds and capacity tests through the shared build queue.
- Check free disk space before large builds or fixtures. Remove unused build
  targets only after checking active processes and container mounts. Preserve
  validation logs/checkpoints and dirty worktrees; remove clean obsolete
  worktrees with `git worktree remove` without `--force` after checking their
  PR dependencies.
- The Dockerfile.dev at repo root is the minimal dev container (no node/pnpm/postgres)
- The full Docker build is at docker/Dockerfile (includes webui, postgres migrations)
- Do NOT hack around build failures with CFLAGS or vendoring overrides

## Backlog workflow

The default branch is `main`. Fetch `origin main` before selection, compare
`git rev-list --left-right --count main...origin/main`, and create ticket
worktrees from `origin/main`. Report unpublished local main commits without
resetting or incorporating them. Use `STR-<number>` in shared skill examples.
Run Streamr tooling from this repository rather than Kyomi's checkout.

Before claiming, run `scripts/check-ticket-in-flight.sh STR-<number>`.
Only exit 0 permits claiming: 1 means existing work, 2 means usage error,
and 3 means incomplete checks. Inspect reported work before proceeding.
Before independent review, repeat from the ticket workspace with
`--self "$BRANCH"` to exclude only your own branch.

Create a unique ticket branch/worktree with:

```bash
git worktree add -b "$BRANCH" "$WORKSPACE" origin/main
```

Use an absolute workspace path and that workspace's scripts. Before pushing,
fetch `origin main`, rebase onto `origin/main` if it advanced, and confirm
the diff contains only ticket work. Preserve other agents' changes.

Run finite builds through `scripts/cargo-dev`, which uses the cooperative
machine-wide `scripts/rust-build` queue. Do not queue servers or watches.
The wrapper uses the existing Bookworm image, Cargo download volumes, and
`target/milestone2-runtime`; build it with `scripts/cargo-dev --build`.

Pre-PR gates (omit `-p` for workspace scope):

```bash
scripts/cargo-dev check --locked -p <crate>
scripts/preflight-clippy.sh -p <crate>
```

The Clippy gate retains CI's flags and adds `--locked`; only exit 0 passes.
Tooling-only changes can be verified without compiling Rust:

```bash
bash scripts/tests/check-ticket-in-flight-self-test.sh
python3 scripts/tests/preflight-clippy-self-test.py
python3 scripts/tests/cargo-dev-self-test.py
```

## Stranded ticket reclamation tooling

Run this repository's scripts from its checkout or ticket worktree. Requirements:
Python 3, Git, and authenticated `gh` access to the actual `origin` repository.
Discover the default branch from `origin`; do not assume it is `main`.

Use `scripts/check-ticket-in-flight.sh TICKET` before claiming work. Only exit 0
permits claiming; reported existing work or incomplete checks must be investigated.

`scripts/mark-worktree-stranded.sh` and `scripts/mark-branch-stranded.sh` are
preservation tools, not abandonment detectors. Run them only after the canonical
`backlog-fast` skill's conservative abandonment checks and per-ticket reservation,
then recheck ownership before writing. An existing PR prevents reclamation.
Preserve uncommitted files, unpublished commits, other agents' claims and validation
evidence. These tools do not remove worktrees or update Trakkt ticket statuses.
Do not use elapsed time alone as proof that an agent has abandoned a ticket.

For tooling-only changes, run:

```bash
python3 -m unittest discover -s scripts/tests -p test_ticket_worktrees.py -v
bash -n scripts/check-ticket-in-flight.sh scripts/mark-worktree-stranded.sh scripts/mark-branch-stranded.sh
```

Product compilation is not needed to validate changes limited to these
Git/worktree tools.

Streamr has no automatic ticket elapsed-time limit. Apply a new limit only when
the user explicitly requests one.
