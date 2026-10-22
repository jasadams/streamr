SET updating_ttl = NULL;
CREATE TABLE events (
 tenant TEXT NOT NULL, k TEXT NOT NULL, event_time TEXT NOT NULL,
 event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
 WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH (connector='single_file',path='$input',format='json',type='source',wait_for_control=true);
-- The three quiet keys and two later filtered events reuse the direct oracle.
-- Optional native relations must survive core maintenance across fresh owners.
CREATE VIEW core AS SELECT tenant,k,
 COUNT(*) FILTER (WHERE CAST(event_ts AS DATE)=EVENT_DATE()) AS total_events
 FROM events GROUP BY tenant,k;
CREATE VIEW optional_a AS SELECT tenant,k,COUNT(*) AS retained
 FROM events WHERE k IN ('a','b','c') GROUP BY tenant,k;
CREATE VIEW optional_b AS SELECT tenant,k,COUNT(*) AS retained
 FROM events WHERE k='b' GROUP BY tenant,k;
CREATE VIEW tagged AS
 SELECT tenant,k,'core' AS kind,total_events,CAST(NULL AS BIGINT) AS optional_a,CAST(NULL AS BIGINT) AS optional_b FROM core
 UNION ALL
 SELECT tenant,k,'a' AS kind,CAST(NULL AS BIGINT) AS total_events,retained AS optional_a,CAST(NULL AS BIGINT) AS optional_b FROM optional_a
 UNION ALL
 SELECT tenant,k,'b' AS kind,CAST(NULL AS BIGINT) AS total_events,CAST(NULL AS BIGINT) AS optional_a,retained AS optional_b FROM optional_b;
CREATE VIEW computed AS SELECT tenant,k,
 MAX(total_events) FILTER (WHERE kind='core') AS total_events,
 MAX(optional_a) FILTER (WHERE kind='a') AS optional_a,
 MAX(optional_b) FILTER (WHERE kind='b') AS optional_b
 FROM tagged GROUP BY tenant,k;
CREATE STATE TABLE snapshots (tenant TEXT,k TEXT,total_events BIGINT,PRIMARY KEY(tenant,k)) PARTITION BY tenant;
CREATE VIEW captured AS MERGE INTO snapshots AS previous USING computed AS incoming
 ON previous.tenant=incoming.tenant AND previous.k=incoming.k
 WHEN MATCHED THEN UPDATE SET total_events=incoming.total_events
 WHEN NOT MATCHED THEN INSERT(tenant,k,total_events) VALUES(incoming.tenant,incoming.k,incoming.total_events)
 RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE results WITH (connector='single_file',path='$output',format='json',type='sink');
CREATE TABLE mirror WITH (connector='single_file',path='$mirror',format='json',type='sink');
INSERT INTO results SELECT source.tenant AS tenant,source.k AS k,source.total_events AS source_total,
 old.total_events AS old_total,new.total_events AS new_total,action,EVENT_TIMESTAMP() AS trigger_time,
 source.optional_a AS optional_a,source.optional_b AS optional_b FROM captured;
INSERT INTO mirror SELECT source.tenant AS tenant,source.k AS k,source.total_events AS source_total,
 old.total_events AS old_total,new.total_events AS new_total,action,EVENT_TIMESTAMP() AS trigger_time,
 source.optional_a AS optional_a,source.optional_b AS optional_b FROM captured;
