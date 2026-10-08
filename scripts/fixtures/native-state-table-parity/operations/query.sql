-- Proposal only: not planned or executed. Substitute SQL-escaped absolute paths for the two tokens.
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, condition BOOLEAN, enabled BOOLEAN) WITH (connector='single_file', path='{{INPUT}}', format='json', type='source', wait_for_control='true');
CREATE TABLE output (id BIGINT, before TEXT, put_value TEXT, upsert_value TEXT, updated BOOLEAN, deleted BOOLEAN, after TEXT) WITH (connector='single_file', path='{{OUTPUT}}', format='json', type='sink');
CREATE STATE TABLE shared_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE VIEW looked AS SELECT e.id, e.key AS map_key, e.payload, e.condition, e.enabled,
m.stored_value AS before FROM events e LEFT JOIN shared_map m ON m.map_key = e.key;
CREATE VIEW put_step AS MERGE INTO shared_map AS target USING looked AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (source.enabled) AND source.payload IS NOT NULL THEN UPDATE SET stored_value = source.payload
WHEN NOT MATCHED AND (source.enabled) AND source.payload IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW after_put AS SELECT source.id AS id, source.map_key AS map_key,
source.payload AS payload, source.condition AS condition, source.before AS before,
CASE WHEN source.enabled AND source.map_key IS NOT NULL THEN source.payload ELSE CAST(NULL AS TEXT) END AS put_value
FROM put_step;
CREATE VIEW seed_step AS MERGE INTO shared_map AS target USING after_put AS source
ON target.map_key = source.map_key
WHEN NOT MATCHED THEN INSERT (map_key, stored_value) VALUES (source.map_key, 'seed')
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW after_seed AS SELECT source.id AS id, source.map_key AS map_key,
source.payload AS payload, source.condition AS condition, source.before AS before,
source.put_value AS put_value, new.stored_value AS upsert_value FROM seed_step;
CREATE VIEW update_step AS MERGE INTO shared_map AS target USING after_seed AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (source.condition) AND source.payload IS NOT NULL THEN UPDATE SET stored_value = source.payload
WHEN NOT MATCHED AND (source.condition) AND source.payload IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW after_update AS SELECT source.id AS id, source.map_key AS map_key,
source.before AS before, source.put_value AS put_value, source.upsert_value AS upsert_value,
action IN ('insert', 'update') AS updated FROM update_step;
CREATE VIEW delete_step AS MERGE INTO shared_map AS target USING after_update AS source
ON target.map_key = source.map_key
WHEN MATCHED AND source.id % 5 = 0 THEN DELETE
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output SELECT d.source.id AS id, d.source.before AS before,
d.source.put_value AS put_value, d.source.upsert_value AS upsert_value,
d.source.updated AS updated, d.action = 'delete' AS deleted, m.stored_value AS after
FROM delete_step d LEFT JOIN shared_map m ON m.map_key = d.source.map_key;
