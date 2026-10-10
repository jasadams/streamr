SET updating_ttl = NULL;
CREATE TABLE join_input (side TEXT NOT NULL, item_id TEXT NOT NULL, k1 TEXT, k2 BIGINT, amount BIGINT NOT NULL, seq BIGINT NOT NULL)
WITH (connector = 'single_file', path = '@DIRECTORY@/input.jsonl', format = 'json', type = 'source', wait_for_control = 'true');
CREATE TABLE join_output (left_id TEXT, right_id TEXT, k1 TEXT, k2 BIGINT, left_total BIGINT, right_total BIGINT, left_count BIGINT, right_count BIGINT)
WITH (connector = 'single_file', path = '@DIRECTORY@/output.jsonl', format = 'debezium_json', type = 'sink');
CREATE VIEW left_values AS SELECT item_id, LAST_VALUE(k1 ORDER BY seq) AS k1, LAST_VALUE(k2 ORDER BY seq) AS k2, SUM(amount) AS total, COUNT(*) AS events FROM join_input WHERE side = 'l' GROUP BY item_id;
CREATE VIEW right_values AS SELECT item_id, LAST_VALUE(k1 ORDER BY seq) AS k1, LAST_VALUE(k2 ORDER BY seq) AS k2, SUM(amount) AS total, COUNT(*) AS events FROM join_input WHERE side = 'r' GROUP BY item_id;
@RESULT_SQL@
