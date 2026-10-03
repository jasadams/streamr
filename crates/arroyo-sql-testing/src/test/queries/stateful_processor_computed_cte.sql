--checkpoint-interval=0
CREATE TABLE events (
  id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT
) WITH (
  connector = 'single_file', path = '$input_dir/stateful_processor.json',
  format = 'json', type = 'source'
);

CREATE TABLE output (
  id BIGINT, normalized TEXT, stored TEXT, retrieved TEXT
) WITH (
  connector = 'single_file', path = '$output_path',
  format = 'json', type = 'sink'
);

-- Reorder fields, prune an unused expression, and execute computed CTE
-- expressions before serializing the state operation keys and values.
INSERT INTO output
WITH prepared AS (
  SELECT unused + 1 AS discarded,
    concat('key:', COALESCE(NULLIF(key, ''), 'blank')) AS map_key,
    CASE WHEN NULLIF(payload, '') IS NULL THEN 'missing'
      ELSE concat('value:', payload) END AS normalized,
    id
  FROM events
  WHERE keep
)
SELECT p.id, p.normalized,
  state_put('computed', p.map_key, p.normalized) AS stored,
  state_get('computed', p.map_key) AS retrieved
FROM prepared AS p;
