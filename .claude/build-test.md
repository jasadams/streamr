# Arroyo Fork Build & Test Procedures

## Build Environment

Arroyo requires Debian Bookworm toolchain. Fedora 44's GCC 16 and OpenSSL 3.5 are incompatible
with vendored C dependencies (sasl2-sys, rdkafka-sys, aws-lc-sys).

**All builds must use the dev container:**

```bash
# Build the dev container (one-time)
podman build -f Dockerfile.dev -t arroyo-dev .

# Run cargo commands inside the container
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo check -p arroyo-worker
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo test -p arroyo-worker
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo clippy -p arroyo-worker -- -D warnings
```

## Crates that build natively on Fedora 44

These crates have no vendored C dependencies and can be checked locally:
- `cargo check -p arroyo-rpc` (proto generation)
- `cargo check -p arroyo-datastream`
- `cargo check -p arroyo-operator`

## Crates that require the dev container

These build native dependencies, including Kafka/SASL or RocksDB:
- `arroyo-worker`
- `arroyo-planner`
- `arroyo-connectors`
- `arroyo-state` (RocksDB, LZ4 and runtime libclang bindings)
- `arroyo` (top-level binary)

## Quick check (per modified crate)

```bash
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo check -p <crate>
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo clippy -p <crate> -- -D warnings
```

## Test commands

```bash
podman run --rm -v "$(pwd):/app:z" arroyo-dev cargo test -p <crate>
```

## Notes
- The Dockerfile.dev at repo root is the minimal dev container (no node/pnpm/postgres)
- The full Docker build is at docker/Dockerfile (includes webui, postgres migrations)
- Do NOT hack around build failures with CFLAGS or vendoring overrides
