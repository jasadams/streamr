# Milestone 2 candidate and validation

This candidate adds opt-in, recoverable disk-backed SQL maps at singleton
parallelism. It is stacked on PR 2 (`299fc34e`), which was open when implementation
started. Milestone 3 retained operator state and milestone 4 routing/rescaling are
separate. No release or deployment is part of this work.

## Runtime contract

Named maps in a linear chain of stateful projections share one ordered execution
owner and one checkpoint boundary. Each input row executes state operations in
expression order, including dependencies on earlier CTE results. The disk path
uses owned reads and admitted writes, commits before emitting that row, and does
not preload maps or retain dirty-key sets. The memory reference stages full state
at every checkpoint, including tombstones, to preserve unchanged keys.

CASE and three-valued AND/OR guards prevent unselected branches from reading or mutating state. Stateful
CASE conditions, lazy COALESCE/NULLIF forms and state stages separated by joins,
filters or branches fail with explicit errors when their ordering is unsupported.
Disk expressions use a bounded supported function set and reject expanding UDFs.
Maps use ASCII letters, digits, underscores and hyphens, with bounded names.

The RocksDB path supports primitive and UTF-8 rows, at most 32 maps per operator,
one database per execution owner, fixed singleton parallelism, and up to 65,536
exclusive logical page files per operator checkpoint, subject also to a 3 MiB
serialized subtask metadata limit beneath the existing 4 MiB RPC receive limit.
Worker execution owners must fit the configured database slots; exhausted slots
fail during worker startup. Oversized input,
intermediate, output or state values fail explicitly. The engine rejects
aggregation/window/join operators under this setting because their retained state
has not been migrated. Connector metadata retains its existing storage and replay
contract.

## Opt-in configuration

The following test-sized example is used by the SQL qualification harness. It is
not a production default. Resource validation requires sufficient headroom for
all admitted SQL producers, nested reads and checkpoint export.

```toml
[worker]
sql-state-backend = "rocksdb"

[worker.disk-sql-state]
directory = "/var/lib/streamr/sql-state"
max-row-bytes = 24576

[worker.live-state-resources]
block-cache-bytes = 8388608
memtable-bytes = 4194304
queued-write-bytes = 1048576
decoded-value-bytes = 1048576
scan-page-bytes = 2097152
max-blocking-operations = 2
max-snapshots = 2
max-open-databases = 2
disk-reserve-bytes = 67108864
```

Assigned state budgets total 12 MiB: 8 MiB cache including memtables, 1 MiB writes,
1 MiB decoding and 2 MiB scans. These budgets do not cap total process RSS.
Runtime input/output queues, DataFusion allocations and bounded checkpoint
metadata also contribute. Qualification measures a separately declared process
RSS envelope.

## Checkpoint and recovery

Both controller metadata and leader manifests carry a versioned disk-map variant.
At the aligned barrier, one stable snapshot captures all maps before later writes.
Bounded pages are uploaded to exclusive checkpoint files with SHA-256 checksums,
schema/encoding identities, ownership, complete file lists and explicit empty
state. Completion is reported after export succeeds. Operator destruction cancels
its exporter and releases captured snapshots.

Recovery restores the protocol-selected checkpoint into a fresh attempt, checking
versions, schemas, original generation, ownership, ordering, sizes and checksums.
A partial import poisons that attempt; retry uses another fresh directory. Local
WAL data never selects committed state. Disposable worker attempts await atomic
write completion without per-row WAL fsync; diagnostic open/reopen retains fsync. Legacy Parquet map checkpoints require an
explicit compatibility error rather than an implicit empty import.

Cleanup keeps retained and active files. Failed deletion preserves publication
metadata for retry. Controller cleanup streams obsolete files. New leader
generations use an immutable append-only publication log: publication and terminal
closure compete for the same next slot. Replacement closes the old generation
before selecting its recovery frontier. Cleanup can then remove uploads excluded
by that closure, including unclaimed page files and full orphan manifests, without
racing a stale publisher. Legacy version-0 generations remain conservative.

Canonical epoch claims, connector commit markers, publication slots and compact
readiness proofs remain as immutable protocol fences after old snapshot data and
large manifests are removed. The small proof keeps retained checkpoints resolvable
after pruning their parent snapshots. Log scanning uses bounded memory but its I/O
grows with checkpoint count; long-running publication latency is not qualified by
this short candidate run. Empty UUID ancestor directories can remain after native
attempt removal. A hard process exit can leave disposable local attempt files;
those files never become a recovery source and need local lifecycle cleanup.

## Reproduce verification

Build and run the documented Bookworm environment. Use a disposable PostgreSQL
build database for controller query generation. The migration helper uses the
locked Refinery library instead of the old downloaded CLI, which requires an
unavailable libssl1.1 on Bookworm. Keep native caches warm:

```sh
podman build -f Dockerfile.dev -t streamr-state-dev .
podman run -d --rm --name streamr-m2-build-db \
  --tmpfs /var/lib/postgresql/data:rw \
  -e POSTGRES_DB=arroyo -e POSTGRES_USER=arroyo -e POSTGRES_PASSWORD=arroyo postgres:16
podman exec streamr-m2-build-db bash -c \
  'for i in {1..60}; do pg_isready -U arroyo && exit 0; sleep 1; done; exit 1'
podman run --rm --network container:streamr-m2-build-db \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  -v "$PWD:/app:z" streamr-state-dev bash scripts/migrate-build-db.sh
milestone2_git_dir=$(git rev-parse --path-format=absolute --git-common-dir)
podman run --rm --network container:streamr-m2-build-db \
  -e DATABASE_URL=postgres://arroyo:arroyo@localhost:5432/arroyo \
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0=/app \
  -v "$PWD:/app:z" -v "${milestone2_git_dir}:${milestone2_git_dir}:ro,z" \
  streamr-state-dev bash scripts/verify-milestone2.sh
```

Controller crate checks require the existing PostgreSQL build database and
migrations used by its query code generation. The test script does not provision
that database or deploy any pipeline.

The SQL tests plan queries and execute the real worker engine, compare explicit
golden outputs, publish checkpoints and recreate operators. Dedicated fault and
memory runs use one process per mode because resource pools are process-global.
The `single_file` test sink truncates to its selected checkpoint offset before
replay; output count and unique IDs detect duplicates in this test contract.
This does not establish delivery guarantees for other connectors.

Qualification writes 32,768 distinct 4 KiB values (128 MiB logical state,
10.67 times the assigned state budget), then performs 128 updates of existing keys and reads with a 25% hot-key skew. A prior-value
oracle verifies that replay begins from the selected state. It
reports checkpoint bytes/time, restore plus replay time, RSS/cardinality samples,
aggregate local test-cache disk bytes, peak RSS, SQL row latency and input-to-emission lag.
Checkpoint timings include barrier alignment and upload. Histogram percentiles
are bucket upper bounds. The test enforces a declared RSS
envelope and a dedicated 600-second runtime deadline per execution phase, asserts
that selected checkpoint 3 exceeds ten times the assigned state
budgets, cleans older checkpoints twice while retaining epoch 3, and verifies complete output after newer uncommitted writes and worker
cancellation.

## Evidence

Local validation on 2026-10-03 used candidate source
`49f0c0ec783d23a32ef735d40da1a13c12c06503` and Bookworm image
`sha256:d9a759860b0921f5fb5b2cab28620b5e3c6e2439c8dbaf8688436dc4ed913785`,
with Rust 1.96 and GCC 12.2. The host was an Intel i5-8259U (4 cores, 8 threads),
31 GiB RAM, 8 GiB swap and NVMe storage with btrfs. The commands above and
`scripts/verify-milestone2.sh` describe the build database, package checks and
isolated runtime modes. [CI on this revision](https://github.com/jasadams/streamr/actions/runs/37107133887)
passed the full Rust 1.95 build, workspace Clippy and console checks, then failed an
existing Kafka source test that rejected a valid idle watermark before partition
assignment. The follow-up changes only that test helper, CI cache handling, the
verification script and this evidence; the qualified runtime is unchanged. A full
CI rerun is required before merge; these local results do not establish CI acceptance.

All 258 unit tests passed against the pinned revision: state 40, protocol 57,
planner 82, worker 47 and RPC 32. Logs are
`/tmp/streamr-m2-arroyo_*-pinned-units.log`.
The seven packages in the verification script passed `cargo check --all-targets`
and `cargo clippy --all-targets --no-deps -- -D warnings`; formatting checks passed.
The pinned memory/controller/leader SQL runs each passed five tests. Controller
and leader each passed the separate crash test, checkpoint-stop test and two fault tests.
Their host logs are `/tmp/streamr-m2-{memory,controller,leader}-sql-final.log` and
`/tmp/streamr-m2-{controller,leader}-{crash,stop,faults}-final.log`.

Both 128 MiB qualification runs passed, producing exactly 65,536 output records
and recovering the selected checkpoint after newer writes and worker cancellation.
Both used a fresh binary built from exact revision
`49f0c0ec783d23a32ef735d40da1a13c12c06503`. Run each mode in a separate process:

```sh
for checkpoint_mode in controller leader; do
  STREAMR_TEST_BACKEND=rocksdb STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" \
    STREAMR_TEST_CRASH=1 STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=600 \
    cargo test --locked -j4 -p arroyo-sql-testing milestone2_larger_than_ram \
      -- --ignored --test-threads=1 --nocapture
done
```

| Measurement | Controller | Leader |
| --- | ---: | ---: |
| Checkpoint 3 bytes | 135,637,781 | 135,637,781 |
| Checkpoint 3 alignment + upload | 21.556 s | 20.686 s |
| Cleanup, two retries retaining epoch 3 | 7.109 s | 0.449 s |
| Initial execution | 144.569 s | 164.511 s |
| Checkpoint execution phase | 77.862 s | 80.045 s |
| Restore + replay | 89.412 s | 44.811 s |
| Qualification total | 313.773 s | 290.797 s |
| Initial throughput, 65,536 / phase seconds | 453.3 rows/s | 398.4 rows/s |
| Peak sampled process RSS | 312.77 MiB | 305.22 MiB |
| SQL row latency mean / p50 / p99 | 1.830 / 2.500 / 10.000 ms | 1.859 / 2.500 / 10.000 ms |
| Input-to-emission lag mean / p50 / p99 | 11.514 / 10 / 60 s | 12.447 / 10 / 60 s |
| Initial RSS near 8k / 16k / 24k cardinality | 205.34 / 209.18 / 211.99 MiB | 204.32 / 207.46 / 211.88 MiB |
| Maximum observed aggregate local cache disk | 240.56 MiB | 257.55 MiB |

Qualification logs are
`/tmp/streamr-m2-controller-qualification-pinned.log` and
`/tmp/streamr-m2-leader-qualification-pinned.log`. Latency percentiles are
histogram bucket upper bounds over 131,074 samples. RSS remained below the
declared 768 MiB envelope; cardinality samples and subsequent fixed-cardinality
updates provide bounded-growth evidence for this workload, not a universal RSS
limit. Local disk samples aggregate the test cache and include roughly
89 million bytes from earlier failed attempts, so they are not per-attempt disk
requirements. The leader recorded 89,380,446 bytes after cleanup and before replay
grew the cache again. Throughput includes planning/startup in the initial phase;
the two modes ran at different times on the shared host and are not a controlled
performance comparison.
