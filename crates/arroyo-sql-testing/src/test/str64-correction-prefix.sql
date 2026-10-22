SET updating_ttl = NULL;
CREATE TABLE changes (
    row_id BIGINT PRIMARY KEY,
    k TEXT NOT NULL,
    value BIGINT NOT NULL,
    active BOOLEAN NOT NULL,
    event_time TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' DAY
) WITH (connector='single_file',path='__INPUT__',format='debezium_json',type='source');
CREATE STATE TABLE totals (
    k TEXT PRIMARY KEY, total BIGINT, peak BIGINT, last_seen TIMESTAMP
) PARTITION BY k;
CREATE VIEW accepted AS
SELECT k, COUNT(*) AS total, MAX(value) AS peak, MAX(event_time) AS last_seen
FROM changes GROUP BY k;
CREATE VIEW applied AS
MERGE INTO totals AS previous USING accepted AS incoming ON previous.k=incoming.k
WHEN MATCHED THEN UPDATE SET total=incoming.total,peak=incoming.peak,last_seen=incoming.last_seen
WHEN NOT MATCHED THEN INSERT (k,total,peak,last_seen)
VALUES (incoming.k,incoming.total,incoming.peak,incoming.last_seen)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output WITH (connector='single_file',path='__OUTPUT__',format='json',type='sink');
INSERT INTO output SELECT applied.source.k AS k,applied.new.total AS total,
 applied.new.peak AS peak,applied.new.last_seen AS last_seen,
 applied.old.total AS old_total,applied.old.peak AS old_peak,
 EVENT_TIMESTAMP() AS trigger_time,applied.action AS action FROM applied;

CREATE TABLE mirror WITH (connector='single_file',path='__MIRROR__',format='json',type='sink');
INSERT INTO mirror SELECT applied.source.k AS k,applied.new.total AS total,
 applied.new.peak AS peak,applied.new.last_seen AS last_seen,
 applied.old.total AS old_total,applied.old.peak AS old_peak,
 EVENT_TIMESTAMP() AS trigger_time,applied.action AS action FROM applied;
