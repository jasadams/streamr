SET updating_ttl = NULL;
CREATE TABLE events (
 k TEXT NOT NULL, value BIGINT NOT NULL, active BOOLEAN NOT NULL,
 event_time TIMESTAMP NOT NULL,
 WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY
) WITH (connector='single_file',path='__INPUT__',format='json',type='source');
CREATE STATE TABLE totals (k TEXT PRIMARY KEY,total BIGINT,peak BIGINT) PARTITION BY k;
CREATE VIEW coherent AS SELECT k,COUNT(*) AS total,
 MAX(value) FILTER (WHERE active) AS peak FROM events GROUP BY k;
CREATE VIEW captured AS
 MERGE INTO totals AS previous USING coherent AS incoming ON previous.k=incoming.k
 WHEN MATCHED THEN UPDATE SET total=incoming.total,peak=incoming.peak
 WHEN NOT MATCHED THEN INSERT(k,total,peak) VALUES(incoming.k,incoming.total,incoming.peak)
 RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output WITH(connector='single_file',path='__OUTPUT__',format='json',type='sink');
CREATE TABLE mirror WITH(connector='single_file',path='__MIRROR__',format='json',type='sink');
INSERT INTO output SELECT applied.source.k AS k,applied.source.total AS source_total,
 applied.source.peak AS source_peak,applied.old.total AS old_total,
 applied.old.peak AS old_peak,applied.new.total AS total,
 applied.new.peak AS peak,EVENT_TIMESTAMP() AS trigger_time,applied.action AS action
 FROM captured AS applied;
INSERT INTO mirror SELECT applied.source.k AS k,applied.source.total AS source_total,
 applied.source.peak AS source_peak,applied.old.total AS old_total,
 applied.old.peak AS old_peak,applied.new.total AS total,
 applied.new.peak AS peak,EVENT_TIMESTAMP() AS trigger_time,applied.action AS action
 FROM captured AS applied;
