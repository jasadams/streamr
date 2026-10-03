# Milestone 3 implementation and verification

This branch builds on milestone 2 PR #4 at `fbd5179a`. It carries the existing
capture harness originating in PR #5 and adds the first STR-16/29 implementation slice:
shared execution accounting, durable dual-clock timers and paginated ranked
collections. These are generic engine capabilities. Application schemas and
business behavior remain in the consuming application. See
[the support matrix](milestone-3-support-matrix.md) for retained paths and
interface gaps requiring discussion before further implementation.

## Execution accounting

```toml
[worker.execution-resources]
memory-bytes = 16777216
max-batch-bytes = 1048576
```

This opt-in test configuration shares one DataFusion memory pool between live
stateless physical executors and projection operators. Plan decoding and execution
use the same runtime. Inputs and outputs have explicit batch limits, and reservations
remain active across collector backpressure. Output streams poll lazily without
prefetch queues. Cancellation, completion and errors release reservations and any
unused input slot. A stateless executor rejects a second input until its previous
stream completes or is dropped. Invalid configuration and budget changes while a
bounded runtime remains live fail explicitly.

The pool accounts cooperative DataFusion execution and these caller boundaries;
it is not a process RSS cap. Kernels allocate output before its batch size can be
checked, and arbitrary UDF allocations are not governed by this pool. Retaining
or cloning a yielded batch after requesting the next one requires caller
accounting. `process_single` returns a caller-owned result and propagates errors.
Generic window/join input channels and histories are not migrated by this slice.
Graph queues retain message-count bounds; byte admission for their retained batches
remains part of STR-16.
Spilling is disabled until disk capacity and cleanup are qualified; resource
exhaustion returns an error. Existing disk SQL retained-operator rejection remains.

## Durable timers and collections

`LiveTable::timers` constructs `DurableTimers` over a registered namespace.
Event and processing clocks have independent primary/deadline indexes within
that same namespace. Replacement, cancellation and related state changes can
commit in one backend batch. Snapshot scans include the exact deadline boundary,
page through arbitrarily many IDs at one timestamp, and bind cursors to the
snapshot, clock and cutoff. Before firing, the operator revalidates the current
deadline/payload against a scanned entry. A stale page cannot cancel a replacement.

`LiveTable::ranked_counts` constructs separately addressed counters and ranking
entries within one registered namespace. Entity keys use length framing; callers
choose and encode logical identity and incarnation where needed. Updating a counter and its previous/new rank is
atomic. Ranking is descending count, then ascending member bytes. This is the generic primitive's
explicit deterministic ordering contract. Counter overflow/underflow and oversized values fail. Top-K reads,
membership scans and retired-incarnation cleanup are bounded by bytes and entries.

Both views require one serial execution owner. Prepared mutations hold that
ownership through the combined state/index commit; they do not implement CAS.
Callers must admit input/related-state assembly and retain prepared-update/page
reservations for their documented lifetimes. `with_resources` validates buffer
and nested-read headroom and charges retained results to the worker state pool.
No hidden namespaces are used, so the milestone 2 full logical snapshot exporter
captures each primary/index relationship together. Schema identities must version
the operator's logical state. Restore always targets a fresh attempt.

These APIs do not schedule callbacks by themselves. An application-owned
processor needs a generic interface to register all state,
drain event timers before forwarding watermarks, fire overdue processing timers
after recovery, emit caller-defined Arrow schemas within admitted limits, and
coordinate output/source/sink checkpoints. The current interface gap must be
discussed before implementation. Storage tests do not establish those callbacks.

## Reproduction

Use the prescribed Bookworm image and migrated build database from
[milestone 2 setup](milestone-2-validation.md#reproduction). Preserve its warm
Cargo target. The first-slice verification command is:

```sh
bash scripts/verify-milestone3.sh
```

The script needs no application checkout. It runs state/RPC/worker units,
SQL-testing compilation, all-target checks, strict Clippy, formatting and diff
checks in the prescribed development container.

External applications may use the opt-in `external_sql_checkpoint_capture` test
with an externally prepared single-file SQL query. The harness accepts absolute
query/output paths and explicit expectations; it has no business schema or oracle.
Set `STREAMR_CAPTURE_QUERY`, `STREAMR_CAPTURE_OUTPUT`,
`STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT`, `STREAMR_CAPTURE_EXPECTED_ROWS`,
`STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS`, and `STREAMR_CAPTURE_CHECKPOINT_EPOCH`.
Use `STREAMR_TEST_BACKEND=memory|rocksdb`,
`STREAMR_TEST_CHECKPOINT_MODE=controller|leader`, and optional
`STREAMR_TEST_EXECUTION_BYTES` for the engine configuration. Run it alone:

```sh
cargo test --locked -j4 -p arroyo-sql-testing external_sql_checkpoint_capture \
  -- --ignored --test-threads=1 --nocapture
```

The source must be a single control-waiting file source; the graph must have
parallelism one. Expected output counts are independent of input counts, so
filters and multiple state owners are not assumed to preserve rows one-for-one.
The initial capture uses `.initial.jsonl`; the recovered capture uses the supplied
output path. The harness recreates the program from the selected published
checkpoint and source/sink metadata. Worker cancellation is not a process-kill,
Kafka, remote-storage or beyond-RAM proof. Fixture preparation, application output
schemas, reference normalization and business comparisons belong externally.

Earlier compatibility evaluation used a pinned external Arcstream fixture at
`10f779469748d0c0dde6d5af3aa7375f8dbc36d3`. Its six initial/recovered identity
comparisons passed at Streamr `a2d2aba7` with 16 MiB execution accounting. That is
historical external evaluation evidence, not an engine dependency or profile/session
readiness claim. The timer and collection tests separately exercise the generic
production logical exporter and fresh RocksDB restoration.

## Validation status

Qualification of `a2d2aba7` passed 153 Bookworm units (33 RPC, 56 state, 64
worker), 25 isolated native fixture runs, all-target checks and strict Clippy.
CI passed 468 library tests and 12 integration tests. Validation for the current
application-boundary correction is recorded against the exact PR head in
[PR #6](https://github.com/jasadams/streamr/pull/6); no pending check counts as
acceptance.

STR-16, STR-29 and the STR-28 inventory remain in progress. STR-17, STR-20, STR-26
and STR-32 retain their full acceptance gates. Milestone 3 requires reviewed
foundation/map artifacts, profile/session parity, actual hot-key and >=10x state
budget qualification, representative backfill and a 24-hour live/fault run before
readiness. No release, deployment or canonical-topic change is part of this branch.
