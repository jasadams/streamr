# Milestone 3 implementation and verification

This branch builds on milestone 2 PR #4 at `fbd5179a`. It carries the existing
identity capture from PR #5 and adds the first STR-16/29 implementation slice:
shared execution accounting, durable dual-clock timers and paginated ranked
collections. It does not establish full Arcstream profile/session readiness.
See [the exact support matrix](milestone-3-support-matrix.md) for the business
contracts, retained paths and remaining implementation owners.

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
include tenant and incarnation. Updating a counter and its previous/new rank is
atomic. Ranking is descending count, then ascending member bytes. This is an
explicit primitive policy; parity with Flink's unspecified equal-count ordering
is not asserted. Counter overflow/underflow and oversized values fail. Top-K reads,
membership scans and retired-incarnation cleanup are bounded by bytes and entries.

Both views require one serial execution owner. Prepared mutations hold that
ownership through the combined state/index commit; they do not implement CAS.
Callers must admit input/related-state assembly and retain prepared-update/page
reservations for their documented lifetimes. `with_resources` validates buffer
and nested-read headroom and charges retained results to the worker state pool.
No hidden namespaces are used, so the milestone 2 full logical snapshot exporter
captures each primary/index relationship together. Schema identities must version
the operator's logical state. Restore always targets a fresh attempt.

These APIs do not schedule callbacks by themselves. Typed profile/session
operators still need to register all state, drain event timers before forwarding
watermarks, fire overdue processing timers on ticks after recovery, construct
native output fields within admitted limits, and coordinate output/source/sink
checkpoints. Storage tests do not establish that operator behavior.

## Reproduction

Use the prescribed Bookworm image and migrated build database from
[milestone 2 setup](milestone-2-validation.md#reproduction). Preserve its warm
Cargo target. The first-slice verification command is:

```sh
ARCSTREAM_REFERENCE_ROOT=/arc bash scripts/verify-milestone3.sh
```

Mount the pinned ARC-16 reference checkout read-only at `/arc`. The inspected
revision is `10f779469748d0c0dde6d5af3aa7375f8dbc36d3`, with identity input/oracle
and preparation/comparison scripts in `test/streamr-reference/`. This command
runs state/RPC/worker units, real identity SQL in memory/controller,
RocksDB/controller and RocksDB/leader modes with 16 MiB execution accounting,
then compares both initial and recovered outputs against the independent Flink
oracle. It also runs all-targets checks, strict Clippy, formatting and diff checks.
Generated captures stay in `target/milestone3-identity/`.

The identity fixture covers 13 events, two directed merges, all 17 forwarded event
fields and checkpoint 41 after event 10. Recovery recreates the program and uses
the selected published checkpoint plus source/sink metadata. It is a small file
connector worker cancellation test, not a process-kill/Kafka/remote-storage or
beyond-RAM qualification. The timer and collection tests separately exercise the
production logical exporter and restoration into fresh RocksDB databases.

## Validation status

The initial coordinated Bookworm run passed 129 tests: 32 RPC, 44 state and 53
worker. Further lifecycle, resource-headroom and collection changes require the
final coordinated rerun. No pending check is treated as passing.

STR-16, STR-29 and the STR-28 inventory remain in progress. STR-17, STR-20, STR-26
and STR-32 retain their full acceptance gates. Milestone 3 requires reviewed
foundation/map artifacts, profile/session parity, actual hot-key and >=10x state
budget qualification, representative backfill and a 24-hour live/fault run before
readiness. No release, deployment or canonical-topic change is part of this branch.
