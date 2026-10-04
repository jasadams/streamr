# Native result composition: observed planner gaps

The generic STR-29 probes below were planned with the fresh combined milestone-3
SQL test executable `arroyo_sql_testing-df802a90a8396c4e`. These are deliberately
rejected capability probes, not supported examples or runtime value evidence.
The input has one key and values 1, 2, 3 at event-time offsets 1, 3, 7 seconds.
A lifetime aggregate must finish at count 3 and sum 6 independently of window
retirement. Source and sink paths refer to disposable capture fixtures.

## Updating aggregate into a state table

```sql
-- Planner/runtime capability probe: updating GROUP BY drives active MERGE.
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '/app/target/str29-native-probes/input.jsonl',
      format = 'json', type = 'source', wait_for_control = 'true');
CREATE STATE TABLE totals (k TEXT PRIMARY KEY, n BIGINT) PARTITION BY k;
CREATE TABLE out (k TEXT, n BIGINT, action TEXT)
WITH (connector = 'single_file', path = '/app/target/str29-native-probes/updating-into-merge.jsonl',
      format = 'json', type = 'sink');
CREATE VIEW grouped AS SELECT k, COUNT(*) AS n FROM events GROUP BY k;
CREATE VIEW applied AS MERGE INTO totals AS target USING grouped AS source
  ON target.k = source.k
  WHEN MATCHED THEN UPDATE SET n = source.n
  WHEN NOT MATCHED THEN INSERT (k, n) VALUES (source.k, source.n)
  RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO out SELECT source.k, new.n, action FROM applied;
```

Actual planner error: `native MERGE source must be an append event stream, not
a changelog` (capture exit 101). An updating aggregate emits changes to an
existing relation. Its retraction is not another append event. Supporting this
route needs explicit keyed replacement/deletion, ordering, checkpoint visibility
and output-capture semantics; removing the append-only check is insufficient.
No new changelog-to-state-table contract has been implemented or approved.

## Closed HOP results into a state table

```sql
-- Planner/runtime capability probe: closed HOP results drive active MERGE.
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp)
WITH (connector = 'single_file', path = '/app/target/str29-native-probes/input.jsonl',
      format = 'json', type = 'source', wait_for_control = 'true');
CREATE STATE TABLE current_window (k TEXT PRIMARY KEY, n BIGINT) PARTITION BY k;
CREATE TABLE out (k TEXT, n BIGINT, action TEXT)
WITH (connector = 'single_file', path = '/app/target/str29-native-probes/hop-into-merge.jsonl',
      format = 'json', type = 'sink');
CREATE VIEW rolling AS
  SELECT k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
         COUNT(*) AS n FROM events GROUP BY k, window;
CREATE VIEW applied AS MERGE INTO current_window AS target USING rolling AS source
  ON target.k = source.k
  WHEN MATCHED THEN UPDATE SET n = source.n
  WHEN NOT MATCHED THEN INSERT (k, n) VALUES (source.k, source.n)
  RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO out SELECT source.k, new.n, action FROM applied;
```

Actual planner error: `state-table fusion: node 4 uses unsupported
SlidingWindowAggregate between related state accesses` (capture exit 101).
Closed windows supply append results, but current fusion rejects this upstream
operator. A bounded result-input boundary must preserve ownership, ordering,
event time and recovery before this path can be qualified. This implementation
restriction is separate from consuming an updating aggregate's changelog.

## Remaining output contracts

Closed HOP windows emit nonempty results; an absent empty window is not an
explicit zero, update or delete. Composition must define how expiry changes a
current rolling result without a new input event. First creation emitted
immediately, subsequent first-pending coalescing, and comparison with the last
emitted snapshot also require precise generic output contracts. An aligned
TUMBLE or an aggregate flush interval does not establish those behaviors.

These findings remain STR-29 work. They do not authorize application-specific
operators, callbacks or output policies in Streamr. See the
[native capability audit](milestone-3-native-capabilities.md) and
[validation record](milestone-3-validation.md) for the wider acceptance limits.
