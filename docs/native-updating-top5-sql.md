# Updating top-five array SQL probe

[`scripts/native_updating_top5_cdc.py`](../scripts/native_updating_top5_cdc.py)
prepared eight generic Debezium cases: memory/RocksDB, controller/leader
checkpoint, and source batches of one/eight. All eight planned, ran, and passed
strict typed CDC and fresh-worker recovery comparison on Streamr
`8649bf440fd55e4cf4289b2bbc72969348b821f7` with executable SHA-256
`35dafbefefc91699a486e686615dbbdc580888e6ff4764ad101a5db75471fa2f`.
The exact case manifest, source inventory and results are at
`target/native-updating-top5-existing-sql/{manifest.json,source-evidence.json,comparisons.json}`.
The existing-SQL core of each case is:

```sql
CREATE VIEW item_counts AS
SELECT k, v AS item_key, COUNT(*) AS n
FROM array_input WHERE v IS NOT NULL GROUP BY k, v;

INSERT INTO ranked_output
SELECT k,
       array_slice(ARRAY_AGG(named_struct('item_key', item_key, 'n', n)
                             ORDER BY n DESC, item_key ASC), 1, 5) AS top_items
FROM item_counts GROUP BY k;
```

The prepared query also declares the keyed Debezium source and typed-Struct
Debezium sink and sets `updating_ttl = NULL`; see the rendered `query.sql` in
each case directory. The comparator derives every expected value from 23 source
events and checks CDC before images, the 21-event checkpoint, and independent
initial and restored final states. Its checkpoint top five for `c` is
`f:2, g:2, a:1, b:1, c:1`; after two deletes it is
`a:1, b:1, c:1, d:1, e:1`. The `b` group remains `y:1` in both phases.

The fixture's intermediate oracle permits each key to move forward through
source-prefix values and assumes the small query's per-item counts become
visible in that order. Exact checkpoint and final maps are checked separately.
This is a focused fixture assumption, not a general guarantee that updates to
different count groups become visible atomically.

`array_slice` limits the emitted array, not the updating `ARRAY_AGG` state.
The engine retains all distinct `(k, v)` counts and the outer aggregate's
members and materializes the full ordered array before slicing. Configured
collection limits may reject high-cardinality groups. This passing matrix
qualifies a small-cardinality array result; it does not provide bounded top-K
state, a hot-key capacity result, or five ranked SQL rows with ordinals.
