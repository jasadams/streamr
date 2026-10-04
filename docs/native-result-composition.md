# Native result composition: observed planner gaps

The investigation concerns engine support for existing SQL. None of the
rejections below establishes a need for new language syntax. Repairing planner
and runtime support takes priority over proposing SQL extensions.

The rejected STR-29 probes below were planned with the fresh combined milestone-3
SQL test executable `arroyo_sql_testing-df802a90a8396c4e`. These are deliberately
rejected capability probes, not supported examples or runtime value evidence.
The input has one key and values 1, 2, 3 at event-time offsets 1, 3, 7 seconds.
A lifetime aggregate must finish at count 3 and sum 6 independently of window
retirement. Source and sink paths refer to disposable capture fixtures.

## Existing SQL: closed windows feeding another aggregate

After repairing the planner's inherited window scope when a projection keeps
only scalar window fields, this existing SQL pattern executes:

```sql
CREATE VIEW closed AS
  SELECT k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
         COUNT(*) AS n FROM events GROUP BY k, window;
CREATE VIEW projected AS
  SELECT k, window.end AS window_end, n FROM closed;
INSERT INTO composed_out
SELECT k, COUNT(*) AS closed_windows, SUM(n) AS pane_memberships,
       MAX(n) AS peak, MAX(window_end) AS latest_end
FROM projected GROUP BY k;
```

`scripts/test-native-window-composition.py` passed all eight combinations of
memory/RocksDB, controller/leader checkpoints and source batch sizes 1/8.
For events at offsets 1, 3 and 7 seconds, both initial and recovered captures
produce exactly five closed windows, six pane memberships, a peak of two and
the final window end at offset 10 seconds. The strict CDC reducer checks old
images as well as final values. Artifacts are in
`target/native-window-composition-current/`.

The executable was built with the finalized-window projection repair and
SESSION/Kafka changes, before the later append-only accumulator patch. This
proves ordinary window-result reaggregation, not a latest-result join with
lifetime totals or an automatic zero for a quiet key. Those remain separate
engine-support and value-verification tasks. No SQL syntax was added.

The same eight captures subsequently passed again on the combined build that
includes bounded append-only accumulator state and native UUID admission;
those artifacts are `target/native-window-composition-combined/`. The combined
build also passed workspace all-target checks, strict Clippy, 604 library tests
(four ignored), and the full workspace build. This does not broaden the
composition fixture's semantic scope.

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


## Existing SQL UNION and outer aggregate

A third probe normalizes lifetime and closed HOP counts into nullable columns,
combines them with `UNION ALL`, then groups by the entity key using existing
`MAX` and ordered `LAST_VALUE`. The exact attempted SQL is:

```sql
SET updating_ttl = NULL;
CREATE TABLE events (timestamp TIMESTAMP NOT NULL, k TEXT NOT NULL, v BIGINT,
  WATERMARK FOR timestamp AS timestamp - INTERVAL '1 minute')
WITH (connector = 'single_file', path = '/app/target/str29-union-probe/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE composed_out (k TEXT, lifetime_count BIGINT, recent_count BIGINT)
WITH (connector = 'single_file', path = '/app/target/str29-union-probe/output.jsonl',
      format = 'debezium_json', type = 'sink');
CREATE VIEW lifetime AS
  SELECT k, COUNT(*) AS lifetime_count FROM events GROUP BY k;
CREATE VIEW rolling AS
  SELECT k, HOP(INTERVAL '2 seconds', INTERVAL '4 seconds') AS window,
         COUNT(*) AS recent_count FROM events GROUP BY k, window;
CREATE VIEW normalized AS
  SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count,
         CAST(NULL AS TIMESTAMP) AS window_end FROM lifetime
  UNION ALL
  SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count,
         window.end AS window_end FROM rolling;
INSERT INTO composed_out
SELECT k, MAX(lifetime_count) AS lifetime_count,
       LAST_VALUE(recent_count ORDER BY window_end)
         FILTER (WHERE recent_count IS NOT NULL) AS recent_count
FROM normalized GROUP BY k;
```

With the native aggregate/window paths enabled, configured memory state,
controller checkpoints and one-row source batches, planning failed before runtime:
`must have window in aggregate. Make sure you are calling one of the windowing
functions (hop, tumble, session) or using the window field of the input`
(capture exit 101). The log is `target/str29-union-probe/capture.log`.

This is a tested planner restriction, not evidence that new SQL syntax is needed.
Even if this query were accepted, an indefinite ordered aggregate would retain
closed-window history, and a nonempty HOP result alone does not create an empty
window's zero. Those semantics remain unqualified.

The [upstream updating-table documentation](https://doc.arroyo.dev/sql/updating-tables/)
and [join documentation](https://doc.arroyo.dev/sql/joins/) were also checked.
Updating join output does not establish arbitrary changelog input support: the
[current upstream join planner](https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-planner/src/plan/join.rs)
rejects updating inputs and mixed windowed/unwindowed inputs. These findings do
not approve any extension; native alternatives and exact remaining semantics
must be investigated first.

### Two-stage current-result UNION: value/recovery and selected-state retention qualified

The bounded candidate first reduces append-only closed windows to one current
result per key, then combines that changing result with the lifetime aggregate:

```sql
CREATE VIEW closed AS
  SELECT k, window.end AS window_end, recent_count FROM rolling;
CREATE VIEW latest_rolling AS
  SELECT k, LAST_VALUE(recent_count ORDER BY window_end) AS recent_count
  FROM closed GROUP BY k;
CREATE VIEW normalized AS
  SELECT k, lifetime_count, CAST(NULL AS BIGINT) AS recent_count FROM lifetime
  UNION ALL
  SELECT k, CAST(NULL AS BIGINT) AS lifetime_count, recent_count FROM latest_rolling;
INSERT INTO composed_out
SELECT k, MAX(lifetime_count) AS lifetime_count,
       MAX(recent_count) AS recent_count
FROM normalized GROUP BY k;
```

This query planned successfully on the combined executable. Its four native
state owners exceeded the capture harness's default two-database admission
limit at startup (`ResourceExhausted`, resource `databases`, limit 2). The probe
was stopped after that failure; it proves planning, not values or recovery.
The log is `/tmp/current-sql-composition-probes/two_stage_union/capture.log`
inside the development container.

The reviewed harness accepts explicit positive test-only database/snapshot
limits, preserving the two-owner defaults. Fresh combined compilation passed.
The runnable query and strict CDC oracle are saved in
`scripts/test-native-result-composition.py`. Its first actual memory/batch-1/
controller case then failed before output: UNION declared `lifetime_count`
non-nullable from the lifetime branch, but its other branch supplies NULL.
The worker rejected the batch with `Column 'lifetime_count' is declared as
non-nullable but contains null values` (capture exit 101, driver exit 1).
The log is `target/native-result-composition-current/memory-1-controller/capture.log`.
This is an existing-language engine schema defect; no values or recovery
passed, and the remaining seven cases did not run. Expected final values
remain lifetime count 3 and latest closed-window count 1. This candidate does
not emit zero automatically when a key becomes quiet, and actual bounded
state retention through UNION still requires runtime evidence.

After the reviewed generic UNION nullability repair, all four memory cases
passed the unchanged strict CDC/value/recovery oracle (batch 1/8 and both
checkpoint protocols). The first RocksDB case rejected the test resource
configuration before startup: four owners with the default 2 MiB scan pool
derive a 19,660-byte checkpoint page, below the 50,176-byte requirement for
two 24 KiB rows plus encoding. The production guard is correct. A reviewed
test-only scan-pool override requests 4 MiB for this fixture, deriving a
72,089-byte page. Its fresh formatting, workspace checks, strict Clippy,
605 library tests and full workspace build passed. The next matrix attempt
stopped in its first memory case on an emission-count assumption: it produced
valid CDC `c {lifetime_count:3,recent_count:NULL}` followed by
`u {lifetime_count:3,recent_count:1}`, rather than one final row. The log/output
are in `target/native-result-composition-scan-pages/memory-1-controller/`.
The initial ready tick of Tokio's interval can race branch arrivals; setting
the subsequent aggregate flush period to 3600 seconds does not suppress that
tick. Production emission/clock behavior remains unchanged. Exact application
creation/debounce expectations remain separate from this generic updating
relation's final-value and recovery verification. RocksDB cases did not run
in this attempt; bounded explicit test-only record counts and strict CDC/value
assertions are under investigation, preserving exact defaults for callers.
Memory artifacts are `target/native-result-composition-nullability/`.

The independently reviewed `scripts/checkpoint_inventory.py` also read all
four actual memory exports, validating selected metadata, checksums, framing
and exact logical keys. The prefix-one checkpoint has lifetime `G=1`, latest
closed-window `G=0`, and outer `G=1,M=2,R=2`, with no dirty/expiry entries.
NULL contributions are indexed too. With both UNION branches live, the
proposed constant outer bound is `M=4,R=4`; a later many-window checkpoint must
prove it. Inventories are saved beside each case as `checkpoint-inventory.json`.
See `docs/native-checkpoint-inventory.md`; this early checkpoint does not
qualify bounded retention through repeated closed-window replacements.

The reviewed current-result fixture then passed all eight memory/RocksDB × batch
target 1/8 × controller/leader cases, preserving the strict CDC, before-image,
checkpoint-prefix and final-value oracle. The prefix has lifetime count 1 and
recent count NULL; final materialized values are 3 and 1. The 64 MiB queued-write
budget is test-only and applies to both generic fixtures. The prior 32 MiB
RocksDB attempt reached runtime: the job and all operators started, then task 17
failed queued-write admission with `ResourceExhausted`
(`queued_write_bytes=33554432`) shortly after startup and before initial
capture or checkpoint. Three aggregate owners each need five 2 MiB queued buffers and the
window owner needs five 0.5 MiB buffers, for at least 32.5 MiB before metadata.
Production admission guards were not changed. Artifacts: `target/native-result-composition-queue-qualified`; log:
`/tmp/streamr-m3-composition-queue-runtime.log`.

The separate four-event memory/batch-1 controller/leader retention probe
inspected checkpoints with `G=1` in each of its three owners and outer `M=4,R=4`.
Its batch-8 companion expected recent count 2 but observed NULL because
watermark generation takes the minimum event time in each source batch,
including values `[1,3,5]`. This fixture expectation is invalid for that batch
shape, not evidence of an engine defect.

At the earlier pre-snapshot-reuse source, the 16-scope retention qualification
had 14 passes: all eight ten-event retraction cases (memory/RocksDB × batch
target 1/8 × controller/leader), all four many-window memory cases (64/4096 groups × controller/leader at batch 8),
and both 64-group RocksDB cases. The artifacts are under
`target/native-result-retention-tail-qualified`. The two scopes then remaining
were 4096-group RocksDB at controller and leader checkpoint modes.

Inventories for 64 and 4096 groups match in both memory checkpoint protocols: all
three owners have `G=1`; latest result, lifetime and outer aggregate values are
1,840, 1,456 and 1,200 bytes respectively, with outer `M=4` entries of 9 bytes
and `R=4` entries of 44 bytes. Total logical value bytes are 4,708. This is
evidence for the tested fixture shape, not unrestricted retention. An earlier
4096-group memory prefix showed recent count 1 instead of 2, with no established
cause; the revised tail fixture asserts latest count 2 for all last-batch
partition lengths 1–8, and the two memory cases now pass.

The first 4096-group RocksDB/controller attempt hit the default 120.24-second
runtime timeout before the initial capture finished. Its incomplete log remains
at `target/native-result-retention-tail-qualified/many-4096-rocksdb-8-controller/capture.log`;
source task completion at 1.6 seconds and lifetime aggregate completion at
55 seconds do not constitute a passing capture. A second controller attempt in
`target/native-result-retention-long-rocksdb` used an explicit 900-second timeout
but failed after 900.27 seconds with `worker runtime timed out`, before any
checkpoint. The source task ended at 1.6 seconds and the lifetime aggregate at
about 30 seconds; window, latest-result and outer aggregate tasks did not finish.
Its incomplete log remains at
`target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller/capture.log`.
The serial `set -e` runner stopped at controller failure, so leader did not run.
Neither historical attempt is a completed capture or performance pass.

A read-only audit of that earlier source suggested snapshot amplification as
one plausible cost of the failed many-window RocksDB run, not a measured or
exclusive bottleneck. Each nonempty interval then looped through
`earliest_time`, `emit_interval` and `expire_before`; each used a window-store
snapshot. `emit_interval` collected one batch into the latest aggregate, whose
`AggregateStore::begin` took another snapshot. RocksDB snapshots create a
physical checkpoint and open a read-only
database. That is roughly four snapshot operations per emitted interval; about
8,000 HOP intervals in the 4,096-group fixture would imply on the order of
32,000 physical snapshot operations. This estimate is source-derived, not
profiling evidence. The historical failing invocation was:

```sh
STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS=900 python3 /app/scripts/test-native-result-retention.py \
  /app/target/native-result-retention-long-rocksdb/many-4096-rocksdb-8-controller \
  --scenario many --groups 4096 \
  --binary /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-df802a90a8396c4e \
  --backend rocksdb --batch 8 --mode controller \
  --checkpoint-root /app/target/native-result-checkpoints \
  --inspector /app/scripts/checkpoint_inventory.py
```

The failed controller run stopped the serial runner before the leader case.
That timeout is preserved and is not a baseline performance pass.

On the fresh combined source, internal point reads avoid a physical snapshot for
native aggregate event chunks that do not scan an index. Native fixed windows
reuse one stable snapshot through each serial watermark pass, with a monotone
expiry cursor that skips rows deleted from the live backend. Stable snapshot
semantics for indexed scans and the public backend interface remain unchanged.
The same batch also added bounded append-only unordered FIRST/LAST and corrected
nested constructor error propagation. Bookworm formatting, workspace all-target
check (16.48 seconds), strict Clippy (18.85 seconds), 25 library suites (610
passed, four ignored), and full workspace build (57.26 seconds) passed. Logs
are `/tmp/streamr-m3-native-first-snapshot-fixed-{fmt,check,clippy,units,build}.log`
inside `streamr-state-build`; the SQL binary was
`arroyo_sql_testing-df802a90a8396c4e` (SHA-256
`b38ee120315ed1490d9bf96de23de44f6f6e697be8809c52eab42f07bbe35364`).

All 16 current-result retention captures now pass on that source under
`target/native-result-retention-snapshot-fixed`: eight retraction cases
(memory/RocksDB × source batch targets 1/8 × controller/leader) and eight
many-window cases (memory/RocksDB × 64/4,096 groups × controller/leader, batch
target 8). The fixture asserts full CDC before-image continuity, exact
checkpoint-prefix/final values, and the 2-to-1 recent-result replacement.
Selected native aggregate namespaces retain exactly 11 keys and 4,708 stored
value bytes at both 64 and 4,096 groups in every backend/protocol pairing;
`target/native-result-retention-snapshot-fixed/comparison-manifest.json`
identifies each capture and inventory by SHA-256. This selected-state proof does
not include window/source/sink state, key/DB overhead, or autonomous quiet-key
zero emission. The 4,096-group RocksDB controller and leader runs completed
initial and recovered captures in 166.55 and 143.94 seconds total,
respectively. Compared with the prior 900.27-second initial timeout, this is
observed completion on the repaired source, not an isolated benchmark or CPU
profile. The earlier 120/900-second failure logs remain available above.

All eight unordered FIRST/LAST generic captures also passed on this source at
`target/native-unordered-first-last-fixed` across both backends, batch targets
1/8 and checkpoint protocols. This is separate from application profile
emission/debounce, the 33-profile and 12-session gates, and the pending fresh
10× TUMBLE capacity qualification.

### Quiet-key zero: independent remaining requirement

Source inspection of the native HOP runtime shows that it skips intervals
without retained panes and groups without partials. Consequently, keeping the
latest nonempty closed result does not make it zero after the key goes quiet.
This is separate from combining that result with lifetime totals.

A diagnostic finite-cadence query was prepared under
`/tmp/streamr-str29-idle-zero-probe/`: event rows at offsets 1/3 seconds, then
caller-supplied non-event cadence rows at 5/9 seconds, with a filtered COUNT
in a 2-second-slide/4-second-width HOP. Its unexecuted oracle predicts counts
2, 1 and 0 for window ends at offsets 4, 6 and 8 seconds. Those cadence rows
instantiate windows and advance watermarks. They do not qualify the required
autonomous timed output; STR-29 forbids fabricating input for that output.
No syntax, API, clock policy or empty-window behavior has been changed on the
strength of this diagnostic. Native engine expiry support and the explicit
application clock/emission requirements still need qualification.

## Time-dependent FILTER: source audit, not a runtime proof

Another existing-SQL expression is a single aggregate:

```sql
SELECT k, COUNT(*) AS lifetime_count,
       COUNT(*) FILTER (
         WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '3 seconds'
       ) AS recent_count
FROM events GROUP BY k;
```

This candidate has not been qualified by a planner/value capture. In the pinned
DataFusion implementation, `CURRENT_TIMESTAMP` and `CURRENT_DATE` simplify to
query-start literals. Streamr applies that simplifier during planning
(`crates/arroyo-planner/src/tables.rs`). Its native aggregate evaluates a filter
on incoming batches; ticks flush dirty groups or expire entire groups through
`updating_ttl`, rather than reevaluating membership and emitting quiet updates
(`crates/arroyo-worker/src/arrow/incremental_aggregator.rs`). Merely accepting
this expression would therefore not prove a moving count.

Changing those clock semantics silently would be inappropriate. A wall-clock
predicate and an event-time HOP window have different meanings. The preferred
investigation remains existing native window and relational composition, with
explicit clock, expiry and bounded-state assertions. New keywords are not a
prerequisite for repairing that engine support.

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
