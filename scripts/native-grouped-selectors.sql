-- Grouped selector contract: raw declared time, SQL secondary keys, write ordinal.
SET updating_ttl = NULL;
CREATE TABLE events (
 row_id BIGINT PRIMARY KEY, canonical_id TEXT NOT NULL, event_time TIMESTAMP NOT NULL, progress_time TIMESTAMP NOT NULL,
 value TEXT, priority BIGINT, payload TEXT NOT NULL,
 WATERMARK FOR event_time AS progress_time - INTERVAL '5' SECOND
) WITH (connector='single_file',path='{{INPUT}}',format='debezium_json',type='source',wait_for_control='true');
CREATE TABLE result (
 fixed_first BIGINT[],fixed_last BIGINT[],fixed_nullable_last BIGINT[],fixed_nonnull_last BIGINT[],
 spread DOUBLE,filtered_spread DOUBLE,midpoint DOUBLE,
 canonical_id TEXT, earliest_value TEXT, latest_value TEXT, descending_value TEXT,
 ordinal_first TEXT, ordinal_last TEXT, tie_first TEXT, interleaved_last TEXT, nulls_first TEXT, nulls_last TEXT,
 latest_nonblank TEXT, assignment TEXT, email TEXT, catalog_email TEXT, device_locale TEXT,
 flag BOOLEAN, number BIGINT, touch_source TEXT, touch_medium TEXT
) WITH (connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink');
CREATE VIEW selected AS SELECT e.canonical_id,
 STDDEV(CAST(1 AS DOUBLE)) AS spread,
 STDDEV(CAST(1 AS DOUBLE)) FILTER (WHERE e.value IS NOT NULL) AS filtered_spread,
 MEDIAN(CAST(e.priority AS DOUBLE)) AS midpoint,
 FIRST_VALUE(arrow_cast(make_array(e.priority, e.priority + 1), 'FixedSizeList(2, Int64)')) AS fixed_first,
 LAST_VALUE(arrow_cast(make_array(e.priority, e.priority + 1), 'FixedSizeList(2, Int64)')) AS fixed_last,
 LAST_VALUE(CASE WHEN e.value IS NULL THEN NULL ELSE arrow_cast(make_array(e.priority, e.priority + 1), 'FixedSizeList(2, Int64)') END) AS fixed_nullable_last,
 LAST_VALUE(CASE WHEN e.value IS NULL THEN NULL ELSE arrow_cast(make_array(e.priority, e.priority + 1), 'FixedSizeList(2, Int64)') END) IGNORE NULLS AS fixed_nonnull_last,
 FIRST_VALUE(e.value) AS earliest_value,
 LAST_VALUE(e.value ORDER BY e.priority ASC) AS latest_value,
 LAST_VALUE(e.value ORDER BY e.priority DESC) AS descending_value,
 FIRST_VALUE(e.value ORDER BY e.priority ASC) AS ordinal_first,
 LAST_VALUE(e.value) AS ordinal_last,
 FIRST_VALUE(e.value ORDER BY e.priority ASC) FILTER (WHERE e.priority=1) AS tie_first,
 LAST_VALUE(e.value) FILTER (WHERE e.row_id>=20) AS interleaved_last,
 LAST_VALUE(e.value ORDER BY e.priority ASC NULLS FIRST) AS nulls_first,
 LAST_VALUE(e.value ORDER BY e.priority ASC NULLS LAST) AS nulls_last,
 LAST_VALUE(NULLIF(e.value,'')) IGNORE NULLS AS latest_nonblank,
 LAST_VALUE(CASE WHEN JSON_EXISTS(e.payload,'$.assignment')
   THEN JSON_VALUE(e.payload,'$.assignment') END)
   FILTER (WHERE JSON_EXISTS(e.payload,'$.assignment')) AS assignment,
 LAST_VALUE(COALESCE(
   NULLIF(JSON_VALUE(e.payload,'$.identifiers[*] ? (@.identity_type == "email").value'),''),
   CASE WHEN JSON_VALUE(e.payload,'$.identity_type')='email'
     THEN NULLIF(JSON_VALUE(e.payload,'$.user_id'),'') END
 ) ORDER BY CASE WHEN NULLIF(JSON_VALUE(e.payload,
   '$.identifiers[*] ? (@.identity_type == "email").value'),'') IS NOT NULL
   THEN 1 ELSE 0 END) IGNORE NULLS AS email,
 LAST_VALUE(
  CASE WHEN JSON_EXISTS(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') AND JSON_VALUE(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') IS DISTINCT FROM '' THEN JSON_VALUE(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value')
  WHEN JSON_VALUE(e.payload, '$.identity_type') = 'email' AND JSON_EXISTS(e.payload, '$.user_id') THEN JSON_VALUE(e.payload, '$.user_id') END
  ORDER BY CASE WHEN JSON_EXISTS(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') AND JSON_VALUE(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') IS DISTINCT FROM '' THEN 1 ELSE 0 END
) FILTER (
  WHERE (JSON_EXISTS(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') AND JSON_VALUE(e.payload, '$.identifiers[*] ? (@.identity_type == "email").value') IS DISTINCT FROM '')
     OR (JSON_VALUE(e.payload, '$.identity_type') = 'email' AND JSON_EXISTS(e.payload, '$.user_id') AND JSON_VALUE(e.payload, '$.user_id') IS DISTINCT FROM '')
) AS catalog_email,
 LAST_VALUE(COALESCE(NULLIF(JSON_VALUE(e.payload,'$.locale'),''),
   NULLIF(JSON_VALUE(e.payload,'$.language'),''))) IGNORE NULLS AS device_locale,
 LAST_VALUE(JSON_VALUE(e.payload,'$.flag' RETURNING BOOLEAN))
   FILTER (WHERE JSON_EXISTS(e.payload,'$.flag')) AS flag,
 LAST_VALUE(CAST(JSON_VALUE(e.payload,'$.number' RETURNING DOUBLE PRECISION) AS BIGINT))
   FILTER (WHERE JSON_EXISTS(e.payload,'$.number')) AS number,
 LAST_VALUE(JSON_QUERY(e.payload,'$.touch'))
   FILTER (WHERE JSON_EXISTS(e.payload,'$.touch')) AS touch
FROM events AS e GROUP BY e.canonical_id;
INSERT INTO result SELECT
 CAST(fixed_first AS BIGINT[]) AS fixed_first,
 CAST(fixed_last AS BIGINT[]) AS fixed_last,
 CAST(fixed_nullable_last AS BIGINT[]) AS fixed_nullable_last,
 CAST(fixed_nonnull_last AS BIGINT[]) AS fixed_nonnull_last,
 spread,filtered_spread,midpoint,canonical_id,earliest_value,latest_value,descending_value,
 ordinal_first,ordinal_last,tie_first,interleaved_last,nulls_first,nulls_last,latest_nonblank,assignment,email,catalog_email,
 device_locale,flag,number,JSON_VALUE(touch,'$.source') AS touch_source,
 JSON_VALUE(touch,'$.medium') AS touch_medium FROM selected;
