# Build and batch QA troubleshooting

Record reproducible build and QA infrastructure issues here so later agents can
reuse the diagnosis. Keep product regressions and ticket acceptance gaps separate
from infrastructure failures. Preserve the initial failed result when retrying.
The [2026-10-08 batch report](batch-qa-2026-10-08.md) records the final coverage
and acceptance gaps after the environment repair.

## OpenSSL generates Solaris assembly on Linux

Observed during batch QA on source
`676475b9e879e4ee7ce5693e839ce40f2e67bb6e` on 2026-10-08.

Symptoms:

```text
openssl-sys 0.9.109 / openssl-src 300.5.0+3.5.0
crypto/aes/aesni-mb-x86_64.s:560: Error: character following name is not '#'
```

The generated line is `.section .note.gnu.property, #alloc`. OpenSSL's
`crypto/perlasm/x86_64-xlate.pl` emits that Solaris syntax when its GNU assembler
probe fails. This does not establish an incompatible GCC or binutils version:
the raw compiler probe succeeds on Debian 12, GCC 12.2 and binutils 2.40.

Confirmed cause: cc-rs automatically adopts `RUSTC_WRAPPER=sccache` for C
compilation. OpenSSL probes the assembler through that wrapper. The first
invocation succeeds; a cache hit attempts to restore its output to `/dev/null`
and fails with `failed to persist temporary file: Resource busy (os error 16)`.
The missing GNU version banner makes OpenSSL choose the Solaris directive.

Reproduce in a disposable Bookworm container with sccache 0.14.0, using its own
cache rather than clearing or modifying a shared cache:

```sh
export SCCACHE_DIR="$(mktemp -d)"
sccache cc -Wa,-v -c -o /dev/null -x assembler /dev/null
sccache cc -Wa,-v -c -o /dev/null -x assembler /dev/null
sccache --stop-server
```

The first call prints the GNU assembler version; the repeated cached call fails.
The corresponding raw `cc` invocation succeeds repeatedly.

Repair: the dev image uses [docker/rustc-cache](../docker/rustc-cache) as
`/usr/local/bin/streamr-rustc-cache`, configured in
[cargo-dev-config.toml](../docker/cargo-dev-config.toml). It forwards Rust
compiler arguments and status to sccache. Its basename is deliberately outside
cc-rs's compiler-wrapper whitelist, so C/C++ probes run directly. Explicit
`CC=cc` alone does not prevent cc-rs's fallback to `RUSTC_WRAPPER=sccache`.
Do not patch generated assembly, change CFLAGS, disable assembly, or change
vendored dependency versions to address this cache-probe failure.

Verification performed:

- Nine `scripts/tests/cargo-dev-self-test.py` tests passed, including shim
  argument and error-status preservation and the minimal image build context.
- An actual cc-rs 1.2.26 build script observed the Rust-only wrapper, selected
  the raw C compiler, and passed three compiler probes and three GNU section
  generation assertions using the locked OpenSSL generator.
- The pinned SQL executable build passed in 14m44s with the repaired image:
  `scripts/cargo-dev test --locked -p arroyo-sql-testing --no-run`, with
  `STREAMR_DEV_IMAGE` set to the immutable image ID below. It produced both
  OpenSSL archives and the actual SQL worker test executable. Its preserved
  SHA-256 is `6711f30fa5448a7cc964844f244dd78187f0292fd218b2bb7d83f0bef57ad5cb`.
  Runtime QA is a separate check; consult the batch result before claiming it
  passed.

The verified local image is
`cdeb96c9e3e929b42343c216ab6ace675c5c321ada45e6b78038c776611fe74e`, tagged
`localhost/streamr-qa-build-env`. This is a local artifact, not a published image.
Build a fresh image from the committed Dockerfile when it is unavailable.

## Required Rust components missing from dev image

Symptom: `cargo fmt` reports that `cargo-fmt` is not installed for Rust 1.96.0.
The Rust base image alone did not provide every repository gate.

Repair: [Dockerfile.dev](../Dockerfile.dev) explicitly installs `rustfmt` and
`clippy` with `rustup component add rustfmt clippy`. Rebuild through
`scripts/cargo-dev --build`; verify `cargo fmt --version` and
`cargo clippy --version` inside that image. Installing host components does not
repair the container. Both version checks passed in the repaired image above.

## Trakkt connector internal errors during outcome recording

Observed on 2026-10-08: initial issue/status/label reads succeeded, but later
`trakkt.get_issue` and `trakkt.list_statuses` repeatedly returned MCP `-32603:
Internal error`. A later bounded retry still failed. The cause is unresolved;
this is not evidence that the batch queue is empty or a product regression.

Keep the batch report and intended outcomes locally. Before any later comment
or status write, successfully re-read current status, labels and comments to
preserve human holds and concurrent changes. Do not invent status IDs, retry
uncertain writes blindly, expose credentials, or claim a ticket was updated.
No writes were attempted during this outage because the required fresh reads
failed. Recheck connector health before resuming ticket recording.

## Aggregate capture fails strict physical row counts

Observed against the preserved SQL executable above on 2026-10-08:

- `test-native-aggregate-ordering.py --total-order`: first native memory/batch-1
  controller checkpoint capture has three rows, while the driver requires two.
- `test-native-unordered-first-last.py`: four memory cases pass, then RocksDB
  batch-1/controller recovery has three rows, while the driver requires two.

The failing assertion is `smoke_tests.rs:2227` in the pinned revision. Logs are
`aggregate-ordering/native-memory-1-controller/capture.log` and
`aggregate-unordered/rocksdb-1-controller/capture.log` under the local evidence
directory. Both drivers set a 3600-second aggregate flush interval. The recorded
extra rows show intermediate changelog updates with continuous before-images;
the unordered recovered stream advances event counts 2 → 3 → 5.

Source review found `tokio::time::interval(period)` in `operator.rs:984`, whose
first tick is immediate. The select loop can consume input before that tick;
`incremental_aggregator.rs:2715` flushes changed groups on the tick and again on
EOF. A long interval therefore does not guarantee a single emission per key.
The same class is already recorded in
[milestone-3-validation.md](milestone-3-validation.md), around line 450: a valid
extra initial CDC update failed an exact row-count assumption.

Independent review classified this evidence as timing-sensitive fixture
cardinality, with no demonstrated FIRST/LAST value regression. Preserve the
failures and any bounded rerun separately, and keep these checks incomplete.
Do not loosen expected counts, drop intermediate rows, or claim the full matrix
passed on the basis of final values alone. No product or assertion change was
made during this batch. A follow-up harness repair should validate every CDC
transition and checkpoint boundary without assuming input wins the first tick;
changing engine emission scheduling would be a separate semantic change.

## Evidence and process cleanup for this incident

An all-target `cargo check` after the SQL executable and selected library tests
started another RocksDB native build under a different Cargo build fingerprint,
despite reusing the same target directory. Quiet Cargo output alone did not mean
the build was stuck: live `cc1plus` commands and increasing object counts showed
progress. Changing package/target/feature scopes can change dependency artifacts;
plan a consistent scope and reserve time for native builds. Do not delete caches
or bypass the shared build queue merely to force visible progress. This is an
observed build cost, not a diagnosed product failure.

Local-only evidence is under
`/home/jason/qa-evidence/streamr-676475b9/`: `sql-build.log`,
`openssl-reproduction.log`, `environment-build.log`, `image-smoke.log`,
`sql-build-retry.log`, `coverage-plan.md` and `result.md`. These files are not
portable repository evidence; confirm their existence before relying on them.
The initial `result.md` records the failed batch, not the later retry's outcome.

Cargo can report one dependency failure while waiting for other native build
jobs. The initial build was stopped only after that failure was diagnosed;
its final exit 137 came from stopping its owned container after graceful stop
timed out. Preserve both the original compiler error and cleanup result.
Never stop containers or builds merely because their names resemble this run.
Use the shared build queue, preserve unrelated work and retain successful
build caches when further validation is expected.
