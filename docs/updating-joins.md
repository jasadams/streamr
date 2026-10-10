# Native updating equijoins

Non-windowed changelog inputs support ANSI `INNER JOIN` and `LEFT JOIN` with a
nonempty `ON` equijoin, including composite keys. Both sides must carry the
existing non-null updating metadata (16-byte row identity and retract flag).
Updating aggregate results and chained updating joins carry that metadata.
Residual predicates, other join types, mixed append/changelog inputs and
windowed updating inputs are rejected. Existing append, window and lookup joins
continue through their existing operators.

A retract must identify and exactly match the retained before-image, including
its timestamp. Retracting an absent identity, inserting an existing identity
without first retracting it, or changing the key in a retract is an error.
Distinct identities with equal values remain distinct SQL bag members. NULL
join-key components do not match. A left row without matches emits its
null-extended result; the first right match retracts that result, and removing
the final right match recreates it. Pair identities derive from the left/right
identities and survive value/key replacement; null-extended identities use a
separate token.

Each input retract or append is processed separately. Replacement pairs and
independent aggregate branches can therefore expose intermediate results;
there is no atomic alignment across branches. Downstream updating operators
must consume every retract/addition, including null-extension transitions.

Configure `worker.join-state` with positive `key-bytes`, `value-bytes`,
`page-bytes`, `page-entries`, `write-bytes`, `write-operations` (at least three),
`overlay-bytes`, `max-pending-output-bytes`, `max-resident-bytes`,
`max-retained-rows` and `max-probe-rows`. Live-state resources and execution
resources are also required. Backend selection uses the same configured memory
or RocksDB lifecycle adapter as other native operators. Both member and
identity indexes plus the retained-row count occupy one registered disk-map
checkpoint namespace; there is no join-specific checkpoint protocol.

Retained identity count and per-change fanout are explicitly limited. Rows,
encoded writes, overlays, decoded probes and one-pair output must fit their
respective budgets. Opposite matches are read through bounded pages; all
predictable probe/output failures are checked before publishing an input
change. Fanout is then streamed one pair at a time under collector backpressure.
Cancellation releases pair/workspace reservations and discards the uncommitted
scope. As with existing native operators, backend commit/IO failure after
collection fails the task; recovery uses the existing source/sink/checkpoint
consistency contract rather than treating already collected rows as a completed
checkpoint.

The semantic reference is upstream
[Arroyo PR 420](https://github.com/ArroyoSystems/arroyo/pull/420): its bag join
processors revise matched pairs on either input, and LEFT joins count the first
and last right matches. The old execution/state implementation is not reused.
[PR 834](https://github.com/ArroyoSystems/arroyo/pull/834) restores nested updating
aggregates rather than joins; the
[0.14 release notes](https://www.arroyo.dev/blog/arroyo-0-14-0/) still defer nested
joins. This implementation restores the missing native operator behavior without
new SQL syntax or an alternate checkpoint path.
