-- Generic existing-SQL assembly; full ARRAY_AGG retained before slicing.
SET updating_ttl = NULL;
CREATE TABLE array_input (
 row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, v TEXT, position BIGINT NOT NULL
) WITH (connector='single_file',path='{{INPUT}}',format='debezium_json',type='source',wait_for_control='true');
CREATE TABLE ranked_output (k TEXT,total BIGINT,top_items STRUCT<item_key TEXT,n BIGINT>[])
WITH (connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink');
CREATE VIEW totals AS SELECT k,COUNT(*) AS total FROM array_input GROUP BY k;
CREATE VIEW counts AS SELECT k,v AS item_key,COUNT(*) AS n FROM array_input WHERE v IS NOT NULL GROUP BY k,v;
CREATE VIEW ranks AS SELECT k,array_slice(ARRAY_AGG(named_struct('item_key',item_key,'n',n) ORDER BY n DESC,item_key ASC),1,5) AS top_items FROM counts GROUP BY k;
CREATE VIEW branches AS
 SELECT k,CAST(NULL AS BIGINT) AS total,top_items,1 AS branch FROM ranks
 UNION ALL
 SELECT k,total,CAST(NULL AS STRUCT<item_key TEXT,n BIGINT>[]) AS top_items,0 AS branch FROM totals;
INSERT INTO ranked_output SELECT k,MAX(total) AS total,
 COALESCE(FIRST_VALUE(top_items ORDER BY branch) FILTER (WHERE top_items IS NOT NULL),CAST(ARRAY[] AS STRUCT<item_key TEXT,n BIGINT>[])) AS top_items
FROM branches GROUP BY k;
