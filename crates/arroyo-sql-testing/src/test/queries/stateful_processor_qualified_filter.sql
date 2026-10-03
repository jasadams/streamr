--checkpoint-interval=0
CREATE TABLE events (
  id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT
) WITH (
  connector = 'single_file', path = '$input_dir/stateful_processor.json',
  format = 'json', type = 'source'
);

CREATE TABLE output (
  id BIGINT, payload TEXT, stored TEXT, retrieved TEXT
) WITH (
  connector = 'single_file', path = '$output_path',
  format = 'json', type = 'sink'
);

-- The filter must execute before the stateful operator, and qualified
-- pass-through columns must retain their physical indices.
-- With checkpoint interval 0, checkpoint 3 saves source lines 1-3. After
-- restoring, row 6 must retrieve key a's original value from checkpointed
-- state; an empty restored map would incorrectly return its payload 'after'.
INSERT INTO output
SELECT e.id, e.payload,
  state_upsert('values', e.key, e.payload) AS stored,
  state_get('values', e.key) AS retrieved
FROM events AS e
WHERE e.keep;
