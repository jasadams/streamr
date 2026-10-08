--checkpoint-interval=0
CREATE TABLE events (
  id BIGINT, key TEXT, payload TEXT, keep BOOLEAN, unused BIGINT
) WITH (
  connector = 'single_file', path = '$input_dir/stateful_processor.json',
  format = 'json', type = 'source'
);

CREATE TABLE output (
  id BIGINT, first_value TEXT, second_value TEXT, final_value TEXT
) WITH (
  connector = 'single_file', path = '$output_path',
  format = 'json', type = 'sink'
);

-- Separate maps deliberately avoid relying on cross-operator map sharing.
-- Each stateful CTE must append distinct result fields and correctly pass
-- qualified values through the next operator.
INSERT INTO output
WITH first_step AS (
  SELECT id, key,
    state_put('first', key, COALESCE(NULLIF(payload, ''), 'missing')) AS first_value
  FROM events
  WHERE keep
), second_step AS (
  SELECT key, first_value, id,
    state_put('second', key, concat(first_value, ':second')) AS second_value
  FROM first_step
)
SELECT id, first_value, second_value,
  state_put('final', key, concat(second_value, ':final')) AS final_value
FROM second_step;
