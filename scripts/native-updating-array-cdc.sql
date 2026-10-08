-- Generic existing-SQL updating ARRAY_AGG qualification fixture.
SET updating_ttl = NULL;
CREATE TABLE array_input (
  row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, v TEXT, position BIGINT NOT NULL
) WITH (connector = 'single_file', path = '$array_input',
        format = 'debezium_json', type = 'source', wait_for_control = 'true');
CREATE TABLE array_output (
  k TEXT, active BIGINT, all_values TEXT[], nonnull_values TEXT[],
  records STRUCT<row_id BIGINT, sort_pos BIGINT, label TEXT>[]
) WITH (connector = 'single_file', path = '$array_output',
        format = 'debezium_json', type = 'sink');
INSERT INTO array_output
SELECT k, COUNT(*) AS active,
  ARRAY_AGG(v ORDER BY position, row_id) AS all_values,
  ARRAY_AGG(v ORDER BY position, row_id) FILTER (WHERE v IS NOT NULL) AS nonnull_values,
  ARRAY_AGG(named_struct('row_id', row_id, 'sort_pos', position, 'label', v)
            ORDER BY position, row_id) AS records
FROM array_input GROUP BY k;
