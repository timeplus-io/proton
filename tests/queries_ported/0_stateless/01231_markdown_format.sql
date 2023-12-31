DROP STREAM IF EXISTS makrdown;
CREATE STREAM markdown (id uint32, name string, array array(int32), nullable nullable(string), low_cardinality low_cardinality(string), decimal Decimal32(6)) ENGINE = Memory;
INSERT INTO markdown VALUES (1, 'name1', [1,2,3], 'Some long string', 'name1', 1.11), (2, 'name2', [4,5,60000], Null, 'Another long string', 222.222222), (30000, 'One more long string', [7,8,9], 'name3', 'name3', 3.33);

SELECT * FROM markdown FORMAT Markdown;
DROP STREAM IF EXISTS markdown
