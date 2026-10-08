#!/usr/bin/env bash
set -euo pipefail
# Run from the repository root against a disposable PostgreSQL build database.
: "${DATABASE_URL:?Set DATABASE_URL for the PostgreSQL build database}"
streamr_migrations_root="$PWD/crates/arroyo-api/migrations"
streamr_migration_helper=$(mktemp -d)
trap 'rm -rf "$streamr_migration_helper"' EXIT
mkdir "$streamr_migration_helper/src"
cp Cargo.lock "$streamr_migration_helper/Cargo.lock"
cat > "$streamr_migration_helper/Cargo.toml" <<'MANIFEST'
[package]
name = "streamr-build-db-migrations"
version = "0.0.0"
edition = "2024"
[workspace]
[dependencies]
refinery = { version = "=0.8.16", features = ["postgres"] }
postgres = "=0.19.10"
MANIFEST
cat > "$streamr_migration_helper/src/main.rs" <<'RUST'
fn main() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let path = std::env::var("STREAMR_BUILD_MIGRATIONS").expect("migration directory");
    let mut client = postgres::Client::connect(&url, postgres::NoTls).expect("connect build database");
    let migrations = refinery::load_sql_migrations(path).expect("load SQL migrations");
    refinery::Runner::new(&migrations).run(&mut client).expect("migrate build database");
}
RUST
STREAMR_BUILD_MIGRATIONS="$streamr_migrations_root" \
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}" \
  cargo run --quiet --manifest-path "$streamr_migration_helper/Cargo.toml"
