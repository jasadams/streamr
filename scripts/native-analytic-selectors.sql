-- Existing analytic contract requires finalized windowed input.
CREATE TABLE events (
 row_id BIGINT, event_time TIMESTAMP NOT NULL, priority BIGINT,
 WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE TABLE result (row_id BIGINT, selected BIGINT)
WITH (connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink');
INSERT INTO result SELECT row_id,
 {{ANALYTIC_EXPRESSION}} AS selected
FROM (
 SELECT row_id,MAX(priority) AS priority,TUMBLE(INTERVAL '10' SECOND) AS window
 FROM events WHERE row_id>0 GROUP BY row_id,window
);
