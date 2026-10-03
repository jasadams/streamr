--checkpoint-interval=1
CREATE TABLE events (
 id BIGINT, key TEXT, payload TEXT, condition BOOLEAN, enabled BOOLEAN
) WITH (
 connector = 'single_file', path = '$input_dir/stateful_processor_operations.json',
 format = 'json', type = 'source'
);
CREATE TABLE output (
 id BIGINT, before TEXT, put_value TEXT, upsert_value TEXT,
 updated BOOLEAN, deleted BOOLEAN, after TEXT
) WITH (
 connector = 'single_file', path = '$output_path', format = 'json', type = 'sink'
);
INSERT INTO output
SELECT id,
 state_get('shared', key) AS before,
 CASE WHEN enabled THEN state_put('shared', key, payload) ELSE CAST(NULL AS TEXT) END AS put_value,
 state_upsert('shared', key, 'seed') AS upsert_value,
 state_update('shared', key, payload, condition) AS updated,
 CASE WHEN id % 5 = 0 THEN state_delete('shared', key) ELSE false END AS deleted,
 state_get('shared', key) AS after
FROM events;
