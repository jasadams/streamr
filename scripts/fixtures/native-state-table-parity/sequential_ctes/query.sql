-- Proposal only: not planned or executed. Substitute SQL-escaped absolute paths for the two tokens.
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT) WITH (connector='single_file', path='{{INPUT}}', format='json', type='source', wait_for_control='true');
CREATE TABLE output (id BIGINT, first_value TEXT, second_value TEXT, final_value TEXT) WITH (connector='single_file', path='{{OUTPUT}}', format='json', type='sink');
CREATE STATE TABLE first_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE STATE TABLE second_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE STATE TABLE final_map (map_key TEXT PRIMARY KEY, stored_value TEXT) PARTITION BY map_key;
CREATE VIEW prepared AS SELECT id, key AS map_key,
COALESCE(NULLIF(payload, ''), 'missing') AS candidate FROM events WHERE keep;
CREATE VIEW first_step AS MERGE INTO first_map AS target USING prepared AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (true) AND source.candidate IS NOT NULL THEN UPDATE SET stored_value = source.candidate
WHEN NOT MATCHED AND (true) AND source.candidate IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW first_values AS SELECT source.id AS id, source.map_key AS map_key,
CASE WHEN source.map_key IS NOT NULL THEN source.candidate ELSE CAST(NULL AS TEXT) END AS first_value FROM first_step;
CREATE VIEW second_input AS SELECT map_key, first_value, id, concat(first_value, ':second') AS candidate FROM first_values;
CREATE VIEW second_step AS MERGE INTO second_map AS target USING second_input AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (true) AND source.candidate IS NOT NULL THEN UPDATE SET stored_value = source.candidate
WHEN NOT MATCHED AND (true) AND source.candidate IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW second_values AS SELECT source.map_key AS map_key, source.first_value AS first_value,
source.id AS id, CASE WHEN source.map_key IS NOT NULL THEN source.candidate ELSE CAST(NULL AS TEXT) END AS second_value FROM second_step;
CREATE VIEW final_input AS SELECT id, map_key, first_value, second_value, concat(second_value, ':final') AS candidate FROM second_values;
CREATE VIEW final_step AS MERGE INTO final_map AS target USING final_input AS source
ON target.map_key = source.map_key
WHEN MATCHED AND (true) AND source.candidate IS NOT NULL THEN UPDATE SET stored_value = source.candidate
WHEN NOT MATCHED AND (true) AND source.candidate IS NOT NULL THEN INSERT (map_key, stored_value) VALUES (source.map_key, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
INSERT INTO output SELECT source.id AS id, source.first_value AS first_value,
source.second_value AS second_value,
CASE WHEN source.map_key IS NOT NULL THEN source.candidate ELSE CAST(NULL AS TEXT) END AS final_value FROM final_step;
