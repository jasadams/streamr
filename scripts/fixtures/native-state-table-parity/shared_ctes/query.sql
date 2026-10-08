-- Proposal only: not planned or executed. Substitute SQL-escaped absolute paths for the two tokens.
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, condition BOOLEAN, enabled BOOLEAN) WITH (connector='single_file', path='{{INPUT}}', format='json', type='source', wait_for_control='true');
CREATE TABLE output (id BIGINT, before TEXT, after TEXT, read_back TEXT) WITH (connector='single_file', path='{{OUTPUT}}', format='json', type='sink');
CREATE STATE TABLE shared_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE VIEW keyed AS SELECT id, key AS map_key FROM events;
CREATE VIEW seed_step AS MERGE INTO shared_map AS target USING keyed AS source
ON target.map_key = source.map_key
WHEN NOT MATCHED THEN INSERT (map_key, stored_value) VALUES (source.map_key, 'seed')
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW initial AS SELECT source.id AS id, source.map_key AS map_key,
new.stored_value AS before FROM seed_step;
CREATE VIEW prepared AS SELECT id, map_key, before, concat(before, ':', CAST(id AS TEXT)) AS candidate FROM initial;
CREATE VIEW put_step AS MERGE INTO shared_map AS target USING prepared AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (true) AND source.candidate IS NOT NULL THEN UPDATE SET stored_value = source.candidate
WHEN NOT MATCHED AND (true) AND source.candidate IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output SELECT p.source.id AS id, p.source.before AS before,
CASE WHEN p.source.map_key IS NOT NULL THEN p.source.candidate ELSE CAST(NULL AS TEXT) END AS after,
m.stored_value AS read_back FROM put_step p LEFT JOIN shared_map m ON m.map_key = p.source.map_key;
