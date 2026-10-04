# Native state-table contract

STR-38 implements declarations and validated catalog metadata. STR-40 plans
direct keyed INNER/LEFT lookups and named continuous MERGE results. STR-41
fuses related accesses into one serial event owner using the configured generic
memory or RocksDB adapter. Native value and fresh-worker recovery captures pass
on both backends and checkpoint modes; see [validation evidence](milestone-3-validation.md).
Broader fault, resource and migration gates remain in STR-42 and STR-43.

## Implemented declarations

```sql
CREATE STATE TABLE inventory (
    tenant TEXT,
    item_id BIGINT,
    quantity BIGINT,
    PRIMARY KEY (tenant, item_id)
) PARTITION BY tenant;
```

`PARTITION BY (tenant, item_id)` and `PARTITION BY tenant, item_id` declare an
ordered composite ownership key. Existing `PARTITIONED BY (tenant)` is also
accepted. Ownership must be a non-empty subset of the primary key, containing
only distinct column names. There is exactly one primary key declaration;
column-level `PRIMARY KEY` is accepted for a single-column key. PRIMARY KEY
implies NOT NULL, and an explicit NULL option on a key is rejected.

Values use the existing SQL-to-Arrow type conversion. Keys permit booleans,
integers, strings, binary values, dates, timestamps and decimal128. Floating,
nested and extension types (including JSON) are rejected as keys. Defaults,
generated columns, secondary constraints, CREATE AS, replacement and conditional
creation are unsupported. No WITH options are accepted, including backend or TTL
options. The configured SQL parallelism must be exactly **1**. This is the
initial fixed ownership contract; changing parallelism or restoring a snapshot
under a different ownership layout must fail rather than repartition silently.

`ArroyoSchemaProvider::register_state_table` validates a parsed declaration and
`get_state_table` returns `state_tables::StateTable`. Metadata includes the Arrow
schema, ordered primary/partition column indices, fixed parallelism, and versioned
table/schema identities. This catalog is separate from connector, memory-result,
and view relations. A name collision in either direction fails. Names use the
existing catalog's case-insensitive UniCase normalization, including quoted names.
The table identity is `state-table-v1:<JSON array of folded identifier values>`.
SQL quote delimiters do not enter names or identities; namespace components
remain separate, so `public.items` differs from `"public.items"`. Schema identity
is a versioned fingerprint of canonical structural JSON containing ordered typed
fields, nullability, sorted extension metadata and ordered key indices. It does
not depend on backend selection or declaration registration order. Runtime row
encoding has its own version and must be checked alongside the full descriptor
on checkpoint recovery; a fingerprint alone is not a schema migration policy.
`validate_partition_compatibility` requires related tables to have the same
ordered partition column names/types and fixed parallelism. Event key expressions
must additionally map to that ownership key in the STR-40 plan.

INSERT into retained state is unsupported; it does not mean append, upsert or
initial population. Standalone MERGE, UPDATE and DELETE are explicitly rejected.
The named CREATE VIEW ... AS MERGE form below executes through the fused owner. No
state mutation falls through to the intermediate memory-table INSERT implementation.

## Native execution contract

A streaming join to a state table is a **current-row keyed lookup driven by an
input event**. Lookup requires equality against the complete primary key and
compatible ownership. It reads the row visible when the event executes, emits
that event's result once, and retains no past input events. A later state update
does not revise earlier outputs. This is distinct from a maintained relational
join, whose historical inputs and changelog behavior cannot be inferred here.

Related table accesses and MERGE outputs belong to one input-event execution
scope at one serial owner. Reads after successful writes in that scope observe
the new value. Reads, mutations and output capture finish in SQL dependency order
before the next input event or checkpoint barrier is processed. Ordinary
independent batch-stage scheduling cannot provide this contract. It requires
operator integration, not a new backend row-transaction abstraction. Checkpoint
replay restores all related state namespaces, ownership, compatible schemas and
pending output progress at one barrier boundary before replaying events; output
delivery guarantees remain tied to the source/sink checkpoint contract.

Key, value, per-event mutation bytes, read/result bytes and captured output rows/
bytes must have explicit positive configured limits. Validate a mutation or
capture against its limit before committing it; exceedance fails the event and
job, never drops data or changes backend. Storage limits are enforced through
existing bounded LiveStateBackend reads/writes and shared resource accounting.
The runtime implementation must define how event failure recovers an already
applied write before claiming event atomicity. This document does not claim that
an asynchronous sequence of backend calls is a transaction.

Default retention is indefinite until explicit deletion. `updating_ttl` applies
to updating aggregates, not these tables. Any future state-table expiry policy
changes application-visible rows and must be declared explicitly with its clock,
replay and checkpoint semantics. No aggregate TTL is inherited.

## Named MERGE output

The syntax below plans and executes through the STR-41 fused serial owner.
Select the required output fields explicitly for a sink:

```sql
CREATE VIEW applied_changes AS
MERGE INTO inventory AS target
USING item_events AS source
ON target.tenant = source.tenant AND target.item_id = source.item_id
WHEN MATCHED THEN UPDATE SET quantity = source.quantity
WHEN NOT MATCHED THEN INSERT (tenant, item_id, quantity)
VALUES (source.tenant, source.item_id, source.quantity)
RETURNING source AS source, old AS old, new AS new, action AS action;

SELECT source.item_id, old.quantity, new.quantity, action
FROM applied_changes;
```

MERGE requires one append event source, equality bindings for every target
primary-key column, and ordered `WHEN MATCHED` UPDATE/DELETE or `WHEN NOT
MATCHED` INSERT clauses. Primary-key updates, target scans, residual ON
predicates, volatile key expressions, independent source joins and batch MERGE
forms are rejected. A direct keyed `INNER JOIN` or `LEFT JOIN` from an event
stream to a state table plans a current-row lookup; scanning a state table alone
is rejected. SQL does not select the memory or RocksDB backend.

The named output is an intermediate streaming relation, not retained storage.
It has one row per input event, including events that match no selected action.
`source` contains the input row; `old` is the nullable previous target row; `new`
is the nullable resulting target row. `action` is one of `insert`, `update`,
`delete`, `none`. Insert has NULL old, delete has NULL new. For no action, old
and new both contain the unchanged matched row, or both NULL if absent. These
names are reserved output fields and require explicit projection/aliases when
combined with ordinary columns. Output names share the relation namespace and
cannot shadow a state table. Multiple downstream consumers reuse the captured
result; consuming the relation never reruns its mutation. Output is bounded and
captured inside the same input-event scope as the mutation. Maintaining a history
of this relation is an application sink decision.

`_timestamp` identifies engine event time in query outputs. A retained table may
store a value with that name; select it with a distinct alias, such as
`target._timestamp AS stored_time`. The current state-table subset rejects a
retained or computed value projected as `_timestamp`, with alias guidance,
instead of treating that value as event time. Direct projection of the actual
event timestamp remains supported. General support for overlapping unaliased
names needs explicit event-time identity throughout projection and materialized
relation planning.

## Backend construction and lifecycle

Tables use `pipeline.sql_state_backend`, whose default is memory. SQL contains
no backend implementation or selection. The existing ownership-aware
`LiveStateBackend`, `LiveTableManager`, worker backend construction, lifecycle
and checkpoint interfaces supply the generic storage boundary. An additional
adapter must meet the same bounded owned reads/writes, read-after-write visibility,
namespace/ownership validation, schema/encoding compatibility and snapshot/restore
contract. Unsupported capability or ownership combinations fail during
construction; no silent memory fallback is allowed. Related namespaces share
one operator-attempt backend and checkpoint boundary. Memory versus RocksDB
must not fork SQL semantics, MERGE rules or event scheduling. Configuring a
backend does not migrate every existing operator to it.
