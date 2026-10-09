STR-61 event-clock worker fixtures

Run the freshly built SQL test executable in the Bookworm container:

    python3 scripts/test-native-event-clock.py /app/target/milestone2-runtime/debug/deps/arroyo_sql_testing-<hash>

The opt-in harness sets the existing source batch linger to one hour so backend
startup cannot split the tiny batch-8 fixture. Size, checkpoint and EOF flushing
retain their existing behavior.

The driver executes actual SQL planning, physical graph reconstruction, initial
execution, checkpoint publication and a fresh worker restore using the existing
`external_sql_checkpoint_capture` test. Forty-eight cases cover memory/RocksDB, source batch targets 1/8 and
controller/leader publication for append, two projected views, populated event-driven state lookup, CDC FOR end_time,
and CDC FOR start_time. It stops on the first failed case and preserves its log
and captures. `--prepare-only` creates reviewable SQL without executing workers;
`--self-test` validates the comparator and fixture consistency only.

The checked-in expected images are an independent oracle, not calculated from
engine captures. Timestamp strings compare all nine fractional digits after
normalizing equivalent UTC ISO formatting. Every output field and every independently expected sink image is checked;
the pre-sink probe independently checks every raw CDC image. For batch 1, replacement envelopes may publish as update or separate
delete/create envelopes; neither representation permits omitted, duplicated,
reordered or altered images. Existing batch-8 sink coalescing collapses all four
operations on one ID into an initial net no-op (exactly zero output rows). The
checkpoint prefix publishes create(corrected b), and the restored suffix
publishes delete(b), whose before image must match that corrected checkpoint
row. These exact sink outputs are independently asserted; raw metadata and
internal IDs still require all six unrolled images before coalescing. Computed
clock outputs for every replacement image are exercised by batch 1; batch 8
checks their checkpoint sink values alongside every pre-sink raw timestamp. The visible session_id must remain 42 through
insert, duration correction, business-day move and delete. Source ts_ms values
are deliberately unrelated to business times. Checkpoint prefix is immediately
after the duration correction, before the next-day replacement.

Append fixtures run watermark-generator DEBUG logging and include a positive
FOR value with negative AS progress (epoch minus two seconds), exercising signed
progress emission and its logging. They distinguish FOR from AS across midnight and across different
columns; include a negative epoch and nanosecond timestamp; and end with arrival
order 20, 10, 30 seconds. The views rename then omit the designated timestamp.
Both CDC conventions require the old retract clock and new addition clock.

The opt-in test-only probe wraps the actual reconstructed watermark owner. It
records the raw Arrow _timestamp and internal CDC _updating_meta.id/is_retract
before sink envelope conversion, plus the actual broadcast Watermark signals.
It also wraps the single-file sink to observe its context watermark immediately
after on_start; the fresh worker must restore the exact checkpoint value,
including epoch minus two seconds, before consuming any suffix rows.
The driver checks every raw image against static golden times and requires one
unchanged 128-bit identity across correction, day move, delete and fresh restore.
Expected progress signals are specified independently for each source batch
size and checkpoint phase, including the partial-batch minimum AS calculation
and the old/new AS minimum within one unrolled CDC replacement;
the EOF sentinel is checked separately from business progress. No production
configuration, state namespace or operator persistence is replaced by the probe.

The lookup case writes a retained timestamp one day behind completeness_time,
then joins that retained state using the event returned by MERGE. Its WHERE
predicate requires the independently expected retained value to exist; complete
output rows still require the trigger's raw FOR clock. This also tests the clock
through MERGE RETURNING, projection and a retained target named _timestamp.

Coverage limits: malformed/absent designation planner diagnostics, standard
CURRENT_DATE/CURRENT_TIMESTAMP, transport fault injection, idleness and maintained
aggregate clock consumption (STR-62) remain separate checks. The internal ID
oracle verifies stable identity across all images/runs, rather than prescribing
a particular hash algorithm. Self-tests use fabricated traces solely to reject
bad comparators; they never establish worker execution.
