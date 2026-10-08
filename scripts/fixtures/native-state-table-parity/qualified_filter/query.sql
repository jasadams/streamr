-- Proposal only: not planned or executed. Substitute SQL-escaped absolute paths for the two tokens.
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT) WITH (connector='single_file', path='{{INPUT}}', format='json', type='source', wait_for_control='true');
CREATE TABLE output (id BIGINT, payload TEXT, stored TEXT, retrieved TEXT) WITH (connector='single_file', path='{{OUTPUT}}', format='json', type='sink');
CREATE STATE TABLE values_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE VIEW filtered AS SELECT e.id, e.key AS map_key, e.payload FROM events e WHERE e.keep;
CREATE VIEW seed_step AS MERGE INTO values_map AS target USING filtered AS source
ON target.map_key = source.map_key
WHEN NOT MATCHED AND source.payload IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.payload)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output SELECT p.source.id AS id, p.source.payload AS payload,
p.new.stored_value AS stored, m.stored_value AS retrieved
FROM seed_step p LEFT JOIN values_map m ON m.map_key = p.source.map_key;
