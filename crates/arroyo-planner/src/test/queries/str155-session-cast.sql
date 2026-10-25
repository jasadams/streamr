SET updating_ttl = NULL;
CREATE TABLE items (k TEXT NOT NULL, sid TEXT NOT NULL, event_time TEXT NOT NULL,
 ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
 WATERMARK FOR ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds')
WITH (connector='kafka',bootstrap_servers='broker:9092',topic='str146-old-64-partitioned-input',format='json',type='source','source.offset'='earliest','source.read_mode'='read_committed','source.group_id'='str146-old-64-partitioned-reader');
CREATE VIEW counts AS SELECT k, COUNT(*) AS n FROM items GROUP BY k;
CREATE VIEW ended AS SELECT sid,SESSION(INTERVAL '30 minutes') AS window,
 FIRST_VALUE(k) AS k,FIRST_VALUE(CAST(ts AS BIGINT)) AS first_stamp,MAX(CAST(ts AS BIGINT)) AS last_stamp
 FROM items GROUP BY sid,window;
CREATE VIEW closed_values AS SELECT k,last_stamp-first_stamp AS duration_ms FROM ended;
CREATE VIEW completions AS SELECT k,COUNT(*) AS completed,SUM(duration_ms) AS duration_ms FROM closed_values GROUP BY k;
CREATE VIEW tagged AS SELECT k,'core' AS kind,n,CAST(NULL AS BIGINT) AS completed,CAST(NULL AS BIGINT) AS duration_ms FROM counts
UNION ALL SELECT k,'closed' AS kind,CAST(NULL AS BIGINT) AS n,completed,duration_ms FROM completions;
CREATE VIEW current_rows AS SELECT k,MAX(n) FILTER(WHERE kind='core') AS n,
 COALESCE(MAX(completed) FILTER(WHERE kind='closed'),CAST(0 AS BIGINT)) AS completed,
 COALESCE(MAX(duration_ms) FILTER(WHERE kind='closed'),CAST(0 AS BIGINT)) AS duration_ms FROM tagged GROUP BY k;
CREATE STATE TABLE snapshots (k TEXT,n BIGINT,completed BIGINT,duration_ms BIGINT,PRIMARY KEY(k)) PARTITION BY k;
CREATE VIEW applied AS MERGE INTO snapshots AS target USING current_rows AS source ON target.k=source.k
WHEN MATCHED THEN UPDATE SET n=source.n,completed=source.completed,duration_ms=source.duration_ms
WHEN NOT MATCHED THEN INSERT(k,n,completed,duration_ms) VALUES(source.k,source.n,source.completed,source.duration_ms)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output(k TEXT,n BIGINT,completed BIGINT,duration_ms BIGINT,action TEXT)
WITH(connector='kafka',bootstrap_servers='broker:9092',topic='str146-old-64-partitioned-output',format='json',type='sink','sink.commit_mode'='exactly_once','sink.recovery_topic'='str146-old-64-partitioned-commits',client_configs='transaction.timeout.ms=60000');
INSERT INTO output SELECT applied.source.k AS k,applied.source.n AS n,applied.source.completed AS completed,applied.source.duration_ms AS duration_ms,applied.action AS action FROM applied;
