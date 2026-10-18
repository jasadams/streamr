-- Append-only windows retain their existing finality and recovery protocol.
CREATE TABLE events (
 row_id BIGINT, canonical_id TEXT NOT NULL, event_time TIMESTAMP NOT NULL,
 value TEXT, priority BIGINT,
 WATERMARK FOR event_time AS event_time - INTERVAL '60' SECOND
) WITH (connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE TABLE result (
 fixed_first BIGINT[],fixed_last BIGINT[],fixed_nullable_last BIGINT[],fixed_nonnull_last BIGINT[],
 spread DOUBLE,filtered_spread DOUBLE,midpoint DOUBLE,
 canonical_id TEXT,start_epoch BIGINT,end_epoch BIGINT,n BIGINT,
 earliest_value TEXT,latest_value TEXT,descending_value TEXT,
 ordinal_first TEXT,ordinal_last TEXT,latest_nonblank TEXT
) WITH (connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO result SELECT
 CAST(fixed_first AS BIGINT[]) AS fixed_first,
 CAST(fixed_last AS BIGINT[]) AS fixed_last,
 CAST(fixed_nullable_last AS BIGINT[]) AS fixed_nullable_last,
 CAST(fixed_nonnull_last AS BIGINT[]) AS fixed_nonnull_last,
 spread,filtered_spread,midpoint,canonical_id,
 CAST(EXTRACT(EPOCH FROM window.start) AS BIGINT) AS start_epoch,
 CAST(EXTRACT(EPOCH FROM window.end) AS BIGINT) AS end_epoch,
 n,earliest_value,latest_value,descending_value,ordinal_first,ordinal_last,latest_nonblank
FROM (
 SELECT canonical_id,{{WINDOW}} AS window,COUNT(*) AS n,
 STDDEV(CAST(1 AS DOUBLE)) AS spread,
 STDDEV(CAST(1 AS DOUBLE)) FILTER (WHERE value IS NOT NULL) AS filtered_spread,
 MEDIAN(CAST(priority AS DOUBLE)) AS midpoint,
 FIRST_VALUE(arrow_cast(make_array(priority, priority + 1), 'FixedSizeList(2, Int64)')) AS fixed_first,
 LAST_VALUE(arrow_cast(make_array(priority, priority + 1), 'FixedSizeList(2, Int64)')) AS fixed_last,
 LAST_VALUE(CASE WHEN value IS NULL THEN NULL ELSE arrow_cast(make_array(priority, priority + 1), 'FixedSizeList(2, Int64)') END) AS fixed_nullable_last,
 LAST_VALUE(CASE WHEN value IS NULL THEN NULL ELSE arrow_cast(make_array(priority, priority + 1), 'FixedSizeList(2, Int64)') END) IGNORE NULLS AS fixed_nonnull_last,
 FIRST_VALUE(value) AS earliest_value,
 LAST_VALUE(value ORDER BY priority ASC) AS latest_value,
 LAST_VALUE(value ORDER BY priority DESC) AS descending_value,
 FIRST_VALUE(value ORDER BY priority ASC) AS ordinal_first,
 LAST_VALUE(value) AS ordinal_last,
 LAST_VALUE(NULLIF(value,'')) IGNORE NULLS AS latest_nonblank
 FROM events WHERE canonical_id<>'control' GROUP BY canonical_id,window
);
