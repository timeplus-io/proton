DROP STREAM IF EXISTS nested_test;
create stream nested_test (x uint32, `nest.col1` array(string), `nest.col2` array(int8)) ENGINE = MergeTree ORDER BY x;

ALTER STREAM nested_test ADD COLUMN `nest.col3` array(LowCardinality(string));
INSERT INTO nested_test (x, `nest.col1`, `nest.col2`) values (1, ['a', 'b'], [3, 4]);
SELECT * FROM nested_test;

DROP STREAM IF EXISTS nested_test;
