--checkpoint-interval=1
CREATE TABLE events (id BIGINT, key TEXT, payload TEXT, condition BOOLEAN, enabled BOOLEAN)
WITH (connector='single_file', path='$input_dir/stateful_processor_operations.json',format='json',type='source');
CREATE TABLE output (id BIGINT, before TEXT, after TEXT, read_back TEXT)
WITH (connector='single_file',path='$output_path',format='json',type='sink');
INSERT INTO output
WITH initial AS (
 SELECT id, key, state_upsert('shared',key,'seed') AS before FROM events
), changed AS (
 SELECT id, key, before, state_put('shared',key,concat(before, ':', CAST(id AS TEXT))) AS after FROM initial
)
SELECT id, before, after, state_get('shared',key) AS read_back FROM changed;
