# Streamr Build & Test Procedures

## Build Environment

Streamr (our Arroyo fork) requires Debian Bookworm toolchain. Fedora 44's GCC 16 and OpenSSL 3.5 are incompatible
with vendored C dependencies (sasl2-sys, rdkafka-sys, aws-lc-sys).

**All builds must use the dev container:**

```bash
# Build the dev container (one-time)
scripts/cargo-dev --build

# Run cargo commands inside the container
scripts/cargo-dev check -p arroyo-worker
scripts/cargo-dev test -p arroyo-worker
scripts/cargo-dev clippy -p arroyo-worker -- -D warnings
```

## Build speed and coordination

Use `scripts/cargo-dev` for finite builds, checks, tests, and Clippy. It queues
builds using the same machine-wide lock as `rust-build` in other repositories,
so independent agents do not launch competing compilations. Direct Cargo or
Podman commands bypass the cooperative queue. Existing running builds are not
interrupted. Do not queue long-running servers or watch commands.

The image uses a pinned Rust 1.96.0 Bookworm toolchain, sccache, and mold. Named
volumes retain Cargo registry/git downloads and a separate 30 GiB container
sccache across disposable containers. Each worktree keeps its own `target/`.
Dev/test builds use incremental workspace compilation and line-table debug
information; external dependencies remain eligible for sccache. Full debugger
type/variable information is reduced. Changing flags requires one initial
rebuild; later builds reuse the new artifacts.

Use `scripts/cargo-dev --stats` to inspect persisted cache size. Cache counters
belong to each container's sccache server and reset between runs. Add
`--timings` to Cargo builds to measure the critical path. `CARGO_BUILD_JOBS`
overrides the default four jobs. The host sccache limit remains 30 GiB.

## Crates that build natively on Fedora 44

These crates have no vendored C dependencies and can be checked locally:
- `rust-build cargo check -p arroyo-rpc` (proto generation)
- `rust-build cargo check -p arroyo-datastream`
- `rust-build cargo check -p arroyo-operator`
- `rust-build cargo check -p arroyo-state`

## Crates that require the dev container

These pull in rdkafka → sasl2-sys (vendored K&R C code):
- `arroyo-worker`
- `arroyo-planner`
- `arroyo-connectors`
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
- The Dockerfile.dev at repo root is the minimal dev container (no node/pnpm/postgres)
- The full Docker build is at docker/Dockerfile (includes webui, postgres migrations)
- Do NOT hack around build failures with CFLAGS or vendoring overrides

## Repository and tracking

- Repository: `jasadams/streamr`
- Local checkout: `/home/jason/repos/streamr`
- Team: Streamr (`STR`)
- Internal crate/binary names remain `arroyo-*` / `arroyo`; `arroyo-dev` is the existing Bookworm build image.
- Stateful SQL currently supports parallelism 1 only. Runtime SQL fixtures retain singleton parallelism during checkpoint recovery.

## Stateful SQL regression suite

```bash
scripts/cargo-dev test --locked -p arroyo-planner stateful
scripts/cargo-dev test --locked -p arroyo-worker test_runtime_
scripts/cargo-dev test --locked -p arroyo-sql-testing stateful_processor -- --test-threads=1
```

The SQL fixtures execute real operators and verify golden output through complete execution, checkpoint/compaction, and restore. The worker checkpoint-staging test alone does not prove durable backend recovery. Cross-stage shared map semantics, conditional state writes, and disk-backed working state are tracked separately as STR-3, STR-4, and STR-5.

## Local candidate packaging

Build the UI first (`pnpm --dir webui install --frozen-lockfile` and
`pnpm --dir webui build`); the API embeds `webui/dist`. The binary build needs
an isolated PostgreSQL database with every `crates/arroyo-api/migrations/V*`
migration applied. Supply its URL through `DATABASE_URL` in the Bookworm
container. Do not use the application database for build-time SQL generation.

Capture the source digest immediately before the final binary build, using the
same pinned Bookworm image for compilation and packaging:

```bash
task_source_sha=$(docker/build-streamr-candidate.sh --source-digest)
# In the pinned Bookworm container with DATABASE_URL configured:
# cargo build --locked -p arroyo --bin arroyo
STREAMR_SOURCE_SHA256="$task_source_sha" \
  docker/build-streamr-candidate.sh target/debug/arroyo localhost/streamr:str-1-candidate
podman run --rm localhost/streamr:str-1-candidate --help
```

The packaging helper rejects intervening source changes and records the source,
original/stripped binary hashes, Git revision, and immutable builder image ID.
It packages the binary, built public console and Swagger assets, and provenance
into a temporary build context. Swagger assets retain their generated build
paths; their file hashes are recorded in the provenance. Debug binaries read console files at their compile-time
`/app/webui/dist` path, so the image includes these assets without source files.
This is a local development candidate retaining the Rust/clang/protoc toolchain
for runtime UDF compilation; the helper does not push or deploy it.

GlobalKeyedTable checkpoints replace the entire snapshot each epoch. The
stateful worker must submit unchanged values and tombstones as well as changed
keys. The repeated-key SQL fixture verifies this through durable restore;
dirty-only checkpoint staging loses unchanged state. STR-5 replaces the current
in-memory working state and remains required before Arcstream qualification.
