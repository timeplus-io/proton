DROP STREAM IF EXISTS tmp;

SET mutations_sync = 2;

create stream tmp (x int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 20;

ALTER STREAM tmp MATERIALIZE COLUMN x; -- { serverError 36 }

ALTER STREAM tmp ADD COLUMN s string DEFAULT to_string(x);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MODIFY COLUMN s string DEFAULT to_string(x+1);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MATERIALIZE COLUMN s;
ALTER STREAM tmp MODIFY COLUMN s string DEFAULT to_string(x+2);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MATERIALIZE COLUMN s;
ALTER STREAM tmp MODIFY COLUMN s string DEFAULT to_string(x+3);
SELECT group_array(x), group_array(s) FROM tmp;
ALTER STREAM tmp DROP COLUMN s;

ALTER STREAM tmp ADD COLUMN s string MATERIALIZED to_string(x);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MODIFY COLUMN s string MATERIALIZED to_string(x+1);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MATERIALIZE COLUMN s;
ALTER STREAM tmp MODIFY COLUMN s string MATERIALIZED to_string(x+2);
SELECT group_array(x), group_array(s) FROM tmp;

ALTER STREAM tmp MATERIALIZE COLUMN s;
ALTER STREAM tmp MODIFY COLUMN s string MATERIALIZED to_string(x+3);
SELECT group_array(x), group_array(s) FROM tmp;
ALTER STREAM tmp DROP COLUMN s;

DROP STREAM tmp;

