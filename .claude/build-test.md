# Arroyo Fork Build & Test Procedures

## Build Environment

Arroyo requires Debian Bookworm toolchain. Fedora 44's GCC 16 and OpenSSL 3.5 are incompatible
with vendored C dependencies (sasl2-sys, rdkafka-sys, aws-lc-sys).

**All builds must use the dev container:**

```bash
# Build the dev container (one-time)
podman build -f Dockerfile.dev -t arroyo-dev .

# Run cargo commands inside the container
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo check -p arroyo-worker
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo test -p arroyo-worker
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo clippy -p arroyo-worker -- -D warnings
```

## Crates that build natively on Fedora 44

These crates have no vendored C dependencies and can be checked locally:
- `cargo check -p arroyo-rpc` (proto generation)
- `cargo check -p arroyo-datastream`

## Crates that require the dev container

These compile native dependencies, including Kafka/SASL or RocksDB:
- `arroyo-worker`
- `arroyo-planner`
- `arroyo-connectors`
- `arroyo-state` (RocksDB, LZ4 and runtime libclang bindings)
- `arroyo-operator` (depends on `arroyo-state`)
- `arroyo` (top-level binary)

## Quick check (per modified crate)

```bash
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo check -p <crate>
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo clippy -p <crate> -- -D warnings
```

## Test commands

```bash
podman run --rm -e CARGO_TARGET_DIR=/app/target/milestone2-runtime -e CARGO_INCREMENTAL=0 -v "$(pwd):/app:z" arroyo-dev cargo test -p <crate>
```

## Notes
- Reuse one container target, `target/milestone2-runtime`, with incremental
  compilation disabled. Serialize builds and capacity runs through the shared
  build queue; do not create another Cargo cache for a ticket worktree.
- Check disk space and active process/container references before large runs or
  removing obsolete worktrees. Preserve uncommitted work and validation evidence.
- The Dockerfile.dev at repo root is the minimal dev container (no node/pnpm/postgres)
- The full Docker build is at docker/Dockerfile (includes webui, postgres migrations)
- Do NOT hack around build failures with CFLAGS or vendoring overrides
