SET updating_ttl = NULL;
CREATE TABLE events (
 tenant TEXT NOT NULL, k TEXT NOT NULL, event_time TEXT NOT NULL,
 event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
 WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH (connector='single_file',path='$input',format='json',type='source',wait_for_control=true);
CREATE VIEW computed AS SELECT tenant,k,
 COUNT(*) FILTER (WHERE CAST(event_ts AS DATE)=EVENT_DATE()) AS total_events
 FROM events GROUP BY tenant,k;
CREATE STATE TABLE snapshots (tenant TEXT,k TEXT,total_events BIGINT,PRIMARY KEY(tenant,k)) PARTITION BY tenant;
CREATE VIEW captured AS MERGE INTO snapshots AS previous USING computed AS incoming
 ON previous.tenant=incoming.tenant AND previous.k=incoming.k
 WHEN MATCHED THEN UPDATE SET total_events=incoming.total_events
 WHEN NOT MATCHED THEN INSERT(tenant,k,total_events) VALUES(incoming.tenant,incoming.k,incoming.total_events)
 RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE results WITH (connector='single_file',path='$output',format='json',type='sink');
CREATE TABLE mirror WITH (connector='single_file',path='$mirror',format='json',type='sink');
INSERT INTO results SELECT source.tenant AS tenant,source.k AS k,source.total_events AS source_total,
 old.total_events AS old_total,new.total_events AS new_total,action,EVENT_TIMESTAMP() AS trigger_time FROM captured;
INSERT INTO mirror SELECT source.tenant AS tenant,source.k AS k,source.total_events AS source_total,
 old.total_events AS old_total,new.total_events AS new_total,action,EVENT_TIMESTAMP() AS trigger_time FROM captured;
