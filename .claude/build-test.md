# Arroyo Fork Build & Test Procedures

Known build and QA infrastructure failures, reproductions and verified repairs
are recorded in [docs/build-qa-troubleshooting.md](../docs/build-qa-troubleshooting.md).
Consult it before repeating a settled investigation, and add new findings with
their source/image versions, evidence and unresolved checks.

## Build Environment

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
