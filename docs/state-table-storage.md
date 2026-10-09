# Typed state-table storage

`arroyo_state::live::typed_table` implements keyed Arrow rows using
`Arc<dyn LiveStateBackend>`. Construction and shutdown belong to adapters;
`worker::construct_backend` selects bounded memory or attempt-scoped RocksDB.
The table kernel does not inspect or downcast the concrete backend. Native SQL
owners reuse generic construction and admitted writes.

A caller supplies `TableDescriptor`: the complete Arrow schema, primary-key
column indices, stable table identity and schema identity. The catalog descriptor
in `arroyo_planner::state_tables` can supply these fields without depending on
storage. Storage currently accepts primitive boolean/integer/float, Utf8, Binary,
Date32/Date64, Timestamp and Decimal128 values; floating-point and Date64 primary
keys, nested/dictionary/extension types fail explicitly. PK columns are distinct
and non-null. Rows contain exactly one row and every declared column; nullable
value fields preserve nulls. Keys contain exactly the projected PK schema.

`LiveTableManager::register_typed` registers each table identity as its durable
namespace and retains its full descriptor. `namespaces()` and
`typed_descriptors()` provide checkpoint enumeration and compatibility metadata.
The namespace preserves operator-attempt backend isolation and explicit ownership.
The initial supported ownership is partition-local subtask 0, parallelism 1.
Other ownership modes and rescaling fail at construction. A standalone
`TypedTable::new` requires its caller to enforce unique ownership and register the
namespace with its checkpoint owner.

Primary keys use Arrow's structured row encoding, prefixed with encoding version
1. Composite keys preserve boundaries, Unicode and full values; routing hashes
never replace keys. Row encoding has the `STRTBL01` version header, schema
identity and an Arrow IPC stream. Reads compare both identity and the complete
schema and reject incompatible bytes. Encoding version is independent of schema
identity. Checkpoint restore must compare the full descriptor, including PK
indices, before admitting an attempt; the schema fingerprint is not a migration
policy. The fused owner registers every typed namespace with TableManager's
checkpoint integration. Full logical snapshots and descriptor/ownership metadata
use the existing controller or leader publication path; a fresh attempt restores
the selected checkpoint into the configured adapter before consuming input.

A working scope holds the shared table-manager owner across awaits. `put`,
`delete` and `get` operate on its table; `put_into`, `delete_from` and `get_from`
operate on other tables registered with the same owner, backend, resources and
limits. Pending operations are visible in order, including repeated replacement,
delete and reinsert. Commit submits one atomic admitted backend batch. Dropping
an uncommitted scope discards its overlay. Completed writes are visible to later
reads. Snapshots wait for the owner and retain their previous committed rows.
There is no input-event durable flush and no SQL transaction abstraction.
Runtime MERGE and lookup operators may share one bounded working scope across
serial input-event steps in an input batch. Each event's decisions and bounded
outputs must be captured before the next event mutates state; pending writes
remain visible through the shared overlay. Commit the bounded backend batch at
a batch or barrier boundary, with barriers between completed events and normal
checkpoint replay after failure. Event ordering does not require a backend
commit or a new transaction for each event. Independent batch-stage scheduling
does not establish this contract.

`TableLimits` explicitly bounds complete encoded keys, encoded rows, decoded
Arrow buffers, scope bytes and operation count, and scan entries/bytes. The scope
reserves worker-wide queue capacity before copying mutations, and reserves its
encoded overlay, metadata and encoding workspace before assembly. Returned rows
keep their batch private and expose `batch()` as borrowed access, retaining
decoded-value permits. Deliberate Arrow buffer clones require additional caller
admission. Scans reserve the full decoded page before any row
is decoded, and share that reservation across retained rows. Caller input buffers
and captured operator outputs remain the caller's responsibility. Holding rows
while requesting additional results can exhaust the shared pool; consumers must
release or explicitly budget retained results. Construction validates combined
scope/read/page workspace headroom. Typed reads, scans and scope construction
use fail-fast resource admission, including nested backend workspace and queue
admission. Exhausted capacity returns an explicit resource error instead of
waiting while holding another pool's permits.
IPC reads preflight every framed message and verify signed metadata/body lengths,
remaining input, exact supported field types, row/node/buffer bounds and EOS
before invoking Arrow's allocating decoder. Compressed, dictionary, nested or
extra-batch messages are rejected; the complete IPC payload must fit the decoded
workspace. Schema and field metadata must match the complete descriptor, with
no duplicate/missing keys; preflight charges the cumulative owned key/value
lengths, field names, timezone strings and object overhead before Arrow can
materialize them. Legal FlatBuffer string aliases cannot bypass this bound.
Raw keyed reads use `min(row_bytes, decoded_bytes)` before backend copying,
matching the encoding ceiling. Construction also bounds catalog identities and
the expanded owned schema representation by the decoded-workspace limit.

Key conversion acquires a separate retained workspace permit before allocating
Arrow row buffers or copying namespace bytes. For each variable field the
checked bound is `4 + ceil(payload_bytes / 32) * 33`; fixed fields use width plus
one byte. The reserved workspace is four times the summed encoded bound, plus
namespace bytes, 1024 bytes per PK column and 1024 bytes of fixed workspace.
This covers the converter buffer and exact-capacity prefixed key together,
allocator rounding, sort/encoding slots and row offsets. The intermediate full
key `to_vec` copy is eliminated. The complete escaped key limit is checked
before copying the final key or namespace. Returned encoded keys retain their
workspace permit through backend access or overlay admission; unused global
headroom is not treated as acquired memory.

The accounted memory adapter has an explicit resident-byte cap, charges retained
key/value bytes plus entry overhead, validates a whole batch before mutation,
and uses worker-wide database and snapshot-count admission. Each snapshot copies
at most the resident cap, so its memory bound is the cap multiplied by admitted
snapshot count, in addition to live resident state. This cap is not a RocksDB
memtable reservation. RocksDB reuses shared cache/memtable/queue/read/scan
budgets, bounded blocking operations, disk-capacity checks and attempt cleanup.
Both adapters validate admitted-write resource-pool identity. Unsupported generic
admission/close capabilities return an explicit error. A future adapter can use
public `AdmittedWriteBatch::reserve`/`into_parts` and
`WorkerStateResources::same_pool`, implement the same handle contract, register
construction, and run conformance. Shutdown requires all live table handles to
be released; snapshots retain their existing backend lifecycle guarantees.

No aggregate TTL applies. Rows remain until explicitly replaced or deleted;
retention is an application-visible policy, not a storage default.

The reusable `typed_table_backend_conformance` test executes the same generic
kernel against bounded memory, RocksDB and a lightweight forwarding test adapter.
It covers composite collision candidates, Unicode, nullable values,
update/delete/reinsert, pending visibility, cross-table namespaces, scope discard,
snapshots/scans, schema mismatch and rejected large input. The separate limits
test verifies resident-cap rejection without mutation and unsupported schema,
ownership and admission. These are storage-contract tests. They do not qualify
native SQL MERGE execution, worker checkpoint/replay or milestone completion.

Review regression tests use tight memory/RocksDB pools with timeout assertions,
concurrent owners under exhausted queue admission, retained-row permit checks,
and hostile IPC lengths, truncated bodies, extra batches and compression headers.

Further allocation regressions cover verified FlatBuffers with aliased schema
and field metadata whose owned expansion exceeds the row reservation, persisted
values larger than the decoded ceiling despite a larger row limit, near-limit
Utf8/Binary key conversion and disproportionate namespace/identity limits.
