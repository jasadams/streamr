-- Proposal only: not planned or executed. Substitute SQL-escaped absolute paths for the two tokens.
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT) WITH (connector='single_file', path='{{INPUT}}', format='json', type='source', wait_for_control='true');
CREATE TABLE output (id BIGINT, normalized TEXT, stored TEXT, retrieved TEXT) WITH (connector='single_file', path='{{OUTPUT}}', format='json', type='sink');
CREATE STATE TABLE computed_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE VIEW prepared AS SELECT unused + 1 AS discarded,
concat('key:', COALESCE(NULLIF(key, ''), 'blank')) AS map_key,
CASE WHEN NULLIF(payload, '') IS NULL THEN 'missing' ELSE concat('value:', payload) END AS normalized,
id FROM events WHERE keep;
CREATE VIEW put_step AS MERGE INTO computed_map AS target USING prepared AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (true) AND source.normalized IS NOT NULL THEN UPDATE SET stored_value = source.normalized
WHEN NOT MATCHED AND (true) AND source.normalized IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.normalized)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output SELECT p.source.id AS id, p.source.normalized AS normalized,
CASE WHEN p.source.map_key IS NOT NULL THEN p.source.normalized ELSE CAST(NULL AS TEXT) END AS stored,
m.stored_value AS retrieved FROM put_step p LEFT JOIN computed_map m ON m.map_key = p.source.map_key;
