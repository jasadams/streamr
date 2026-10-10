# Native updating join fixture driver

`scripts/test-native-updating-joins.py` uses the existing
`external_sql_checkpoint_capture` SQL test hook. It accepts caller-supplied JSON
fixtures with `--fixture`; no application checkout or schema is required. The
repository examples use one controlled append source, independent filtered
updating aggregates, and existing ANSI INNER/LEFT composite equijoin SQL.

Prepare only:

```sh
python3 scripts/test-native-updating-joins.py /absolute/evidence/fixtures
```

Run all 32 cases using a coordinator-pinned executable and its source/build
receipt, within the coordinator's existing shared queue reservation:

```sh
python3 scripts/test-native-updating-joins.py /absolute/evidence/matrix \
  --binary /absolute/pinned/arroyo-sql-testing \
  --source-receipt /absolute/evidence/source-build-receipt.json
```

The driver enables `STREAMR_TEST_NATIVE_UPDATING_JOINS=1` together with native
aggregates. The smoke hook explicitly sets join limits: 100,000 retained rows,
10,000 probe rows, 512-byte keys, 32 KiB values, 128 KiB/64-entry pages,
2 MiB/128-operation writes, 2 MiB overlay, 512 KiB pending output, and
128 MiB resident state. It allows eight open backend databases. These finite
fixture limits are configured by the pinned test binary and bound to its source
receipt, rather than product defaults. The driver explicitly selects
`STREAMR_TEST_SCAN_PAGE_BYTES=16777216` (16 MiB) through the existing test
hook. With eight configured database owners, checkpoint page admission uses
`scan_page_bytes / (8 * (max_open_databases + 1)) - 32768`, with saturating
subtraction and additional page/write caps. A 2 MiB scan budget yields zero;
the scan component needs at least 2,359,368 bytes for a positive integer page.
The explicit 16 MiB fixture budget satisfies that
existing checkpoint contract; product limits and checkpoint logic are unchanged.
The driver also selects `STREAMR_TEST_QUEUED_WRITE_BYTES=67108864` (64 MiB).
`AdmittedWriteBatch::reservation_bytes` charges five times the configured write
bytes plus per-operation overhead: each 2 MiB/128-operation owner reserves
slightly over 10 MiB. The maximum actual graph across these four fixture
variants has four state owners, so overlapping reservations need slightly over
40 MiB. The explicit 64 MiB pool provides headroom for operation overhead and
checkpoint work. The eight-database admission ceiling does not claim a fixture
with eight concurrent owners. Product admission, operator limits and accounting
are unchanged.
Receipts include `driver_sha256`, binding this harness configuration to the
pinned engine executable and its independently preserved source/build receipt.
The initial failed capture must remain in a separate evidence directory when
rerunning the same pinned executable with this scan configuration.

Four fixture variants run the direct INNER/LEFT pair join, an INNER pair join
followed by another INNER join to the right aggregate on its item identity, and
a keyed updating SUM aggregate over the INNER pair join. The optional
`query_template` field accepts only `direct`, `chained-inner`, or
`downstream-inner`; composed templates require INNER. Each template uses the
same eight visible columns and independently expected final relational bag.

The matrix selects memory/RocksDB, controller/leader checkpoints, and source
batches 1/8. Every case preserves SQL, input, fixture and configuration hashes,
pinned executable hash, supplied source receipt and hash, capture log, and result
status. A supplied receipt records provenance; the coordinator remains responsible
for ensuring it describes the actual executable. Preparation does not count as a
runtime pass. The evidence root and each case directory must be absent or empty; existing
captures are never overwritten. Capture failures remain marked `capture-failed`;
oracle failures retain `oracle-failed` and the assertion error. Only successful
capture followed by all oracle assertions produces `passed`.

The independent oracle derives SUM/COUNT/latest ordered key values from input
prefixes. Key values come from the greatest `seq` per side/item group, even
when events arrive out of order; SUM and COUNT include every arrived event.
Duplicate `seq` within a side/item group is rejected before preparation or
execution because its `LAST_VALUE` result has no declared tie breaker.
Equal `seq` in distinct groups is allowed. The oracle then computes relational equijoin results. NULL never equals NULL.
Every CDC old value must exactly equal the currently held value for its observable
left/right pair; creates cannot overwrite a live pair, and deletes cannot remove
an absent pair. Complete new values must match forward independently advancing
left/right prefixes. It retains all possible prefix positions rather than choosing
an arbitrary timing alignment. Immediate aggregate flush ticks and independent
branch scheduling can change record counts, so cardinality is bounded rather
than fixed. Every complete checkpoint and final bag must exactly equal the
independently computed values and multiplicities. Fanout may publish several
records, so intermediate bags are not required to be atomic relational snapshots.
Every variant allows deletion/recreation between a genuine upstream retraction
and replacement, including an unchanged visible pair. `UpdatingJoin::emit`
publishes individual rows; `ToDebeziumStream` in `physical.rs` coalesces metadata
only within one batch, so separate retract/add batches can become sink deletes
and creates. Native keyed GROUP BY also removes a group after its final live
input is retracted (`incremental_aggregator.rs`, `native_live_rows_key`). Each
deletion must exactly match its live old row and preserves its last complete-value
prefix frontier; it does not prove a new source-prefix absence or authorize a
backwards recreation. Every later create must still match independently derived
forward complete values, and checkpoint/final bags remain exact.
Sink-visible pair columns verify pair continuity; internal metadata identity is
also the responsibility of worker tests.

The examples cover updates on each branch, one-to-many and repeated join keys,
composite key isolation, nullable components, join-key changes, matched output
removals/recreation, and LEFT first-match/last-match transitions. Row 8 gives an
empty INNER checkpoint and nonempty LEFT checkpoint, followed by fresh-worker
continuation and additional key changes. A zero-amount event preserves SUM while
COUNT changes. SQL groups themselves do not disappear on an append source.

Acceptance still requires engine tests for aggregate-group deletion/recreation,
unchanged whole rows, internal
stable identities, bounded fanout during checkpoint, resource-limit failures,
slow consumers and cancellation. This driver does not claim those scenarios or
capacity/process-loss qualification.

Development validation:

```sh
python3 scripts/tests/native-updating-joins-self-test.py
python3 -m py_compile scripts/test-native-updating-joins.py scripts/tests/native-updating-joins-self-test.py
```

The self-tests exercise valid coalescing, independent branch timing, exact final
and checkpoint bags, null semantics and LEFT transitions, then deliberately
corrupt complete values, old values, duplicate creates, final multiplicity and
forward progress to ensure rejection. They also verify composed SQL selection,
template rejection and temporary vacancies that preserve the value frontier. These are oracle checks, not SQL execution.
