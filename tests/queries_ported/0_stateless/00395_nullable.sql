SELECT '----- NULL value -----';

SELECT NULL;
SELECT 1 + NULL;
SELECT abs(NULL);
SELECT NULL + NULL;

SELECT '----- MergeTree engine -----';

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(
col1 uint64, col2 nullable(uint64),
col3 string, col4 nullable(string),
col5 array(uint64), col6 array(nullable(uint64)),
col7 array(string), col8 array(nullable(string)),
d date) Engine = MergeTree(d, (col1, d), 8192);

INSERT INTO test1_00395 (col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 (col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 (col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 (col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395 (col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;


SELECT '----- Memory engine -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(
col1 uint64, col2 nullable(uint64),
col3 string, col4 nullable(string),
col5 array(uint64), col6 array(nullable(uint64)),
col7 array(string), col8 array(nullable(string)),
d date) Engine = Memory;

INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- TinyLog engine -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(
col1 uint64, col2 nullable(uint64),
col3 string, col4 nullable(string),
col5 array(uint64), col6 array(nullable(uint64)),
col7 array(string), col8 array(nullable(string)),
d date) ;

INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, NULL, 'a', 'a', [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', NULL, [1], [1], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [NULL], ['a'], ['a'], '2000-01-01');
INSERT INTO test1_00395(col1, col2, col3, col4, col5, col6, col7, col8, d) VALUES (1, 1, 'a', 'a', [1], [1], ['a'], [NULL], '2000-01-01');
SELECT sleep(3);
SELECT * FROM test1_00395 ORDER BY col1,col2,col3,col4,col5,col6,col7,col8 ASC;

SELECT '----- Insert with expression -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64))) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES ([1+1]);
SELECT col1 FROM test1_00395 ORDER BY col1 ASC;

SELECT '----- Insert. Source and target columns have same types up to nullability. -----';
DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint64), col2 uint64) Engine=Memory;
DROP STREAM IF EXISTS test2;
create stream test2(col1 uint64, col2 nullable(uint64)) Engine=Memory;
INSERT INTO test1_00395(col1,col2) VALUES (2,7)(6,9)(5,1)(4,3)(8,2);
INSERT INTO test2(col1,col2) SELECT col1,col2 FROM test1_00395;
SELECT col1,col2 FROM test2 ORDER BY col1,col2 ASC;

SELECT '----- Apply functions and aggregate functions on columns that may contain null values -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint64), col2 nullable(uint64)) Engine=Memory;
INSERT INTO test1_00395(col1,col2) VALUES (2,7)(NULL,6)(9,NULL)(NULL,NULL)(5,1)(42,42);
SELECT col1, col2, col1 + col2, col1 * 7 FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT sum(col1) FROM test1_00395;
SELECT sum(col1 * 7) FROM test1_00395;

SELECT '----- isNull, isNotNull -----';

SELECT col1, col2, is_null(col1), is_not_null(col2) FROM test1_00395 ORDER BY col1,col2 ASC;

SELECT '----- if_null, null_if -----';

SELECT col1, col2, if_null(col1,col2) FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT col1, col2, null_if(col1,col2) FROM test1_00395 ORDER BY col1,col2 ASC;
SELECT null_if(1, NULL);

SELECT '----- coalesce -----';

SELECT coalesce(NULL);
SELECT coalesce(NULL, 1);
SELECT coalesce(NULL, NULL, 1);
SELECT coalesce(NULL, 42, NULL, 1);
SELECT coalesce(NULL, NULL, NULL);
SELECT col1, col2, coalesce(col1, col2) FROM test1_00395 ORDER BY col1, col2 ASC;
SELECT col1, col2, coalesce(col1, col2, 99) FROM test1_00395 ORDER BY col1, col2 ASC;

SELECT '----- assumeNotNull -----';

SELECT res FROM (SELECT col1, assume_not_null(col1) AS res FROM test1_00395) WHERE col1 IS NOT NULL ORDER BY res ASC;

SELECT '----- IS NULL, IS NOT NULL -----';

SELECT col1 FROM test1_00395 WHERE col1 IS NOT NULL ORDER BY col1 ASC;
SELECT col1 FROM test1_00395 WHERE col1 IS NULL;

SELECT '----- if -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395 (col1 nullable(string)) ;
INSERT INTO test1_00395(col1) VALUES ('a'), ('b'), ('c'), (NULL);
SELECT sleep(3);
SELECT col1, if(col1 IN ('a' ,'b'), 1, 0) AS t, to_type_name(t) FROM test1_00395;
SELECT col1, if(col1 IN ('a' ,'b'), NULL, 0) AS t, to_type_name(t) FROM test1_00395;

SELECT '----- case when -----';

SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 ELSE 0 END AS t, to_type_name(t) FROM test1_00395;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN NULL ELSE 0 END AS t, to_type_name(t) FROM test1_00395;
SELECT col1, CASE WHEN col1 IN ('a' ,'b') THEN 1 END AS t, to_type_name(t) FROM test1_00395;

SELECT '----- multi_if -----';

SELECT multi_if(true, NULL, true, 3, 4);
SELECT multi_if(true, 2, true, NULL, 4);
SELECT multi_if(NULL, NULL, NULL);

SELECT multi_if(true, 'A', true, NULL, 'DEF');
SELECT multi_if(true, to_fixed_string('A', 16), true, NULL, to_fixed_string('DEF', 16));

SELECT multi_if(NULL, 2, true, 3, 4);
SELECT multi_if(true, 2, NULL, 3, 4);

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(int8), col2 nullable(uint16), col3 nullable(float32)) Engine=Memory;
INSERT INTO test1_00395(col1,col2,col3) VALUES (to_int8(1),to_uint16(2),to_float32(3))(NULL,to_uint16(1),to_float32(2))(to_int8(1),NULL,to_float32(2))(to_int8(1),to_uint16(2),NULL);
SELECT multi_if(col1 == 1, col2, col2 == 2, col3, col3 == 3, col1, 42) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(cond1 nullable(uint8), then1 int8, cond2 uint8, then2 nullable(uint16), then3 nullable(float32)) Engine=Memory;
INSERT INTO test1_00395(cond1,then1,cond2,then2,then3) VALUES(1,1,1,42,99)(0,7,1,99,42)(NULL,6,2,99,NULL);
SELECT multi_if(cond1,then1,cond2,then2,then3) FROM test1_00395;

SELECT '----- array functions -----';

SELECT [NULL];
SELECT [NULL,NULL,NULL];
SELECT [NULL,2,3];
SELECT [1,NULL,3];
SELECT [1,2,NULL];

SELECT [NULL,'b','c'];
SELECT ['a',NULL,'c'];
SELECT ['a','b',NULL];

SELECT '----- array_element -----';

SELECT '----- constant arrays -----';

SELECT array_element([1,NULL,2,3], 1);
SELECT array_element([1,NULL,2,3], 2);
SELECT array_element([1,NULL,2,3], 3);
SELECT array_element([1,NULL,2,3], 4);

SELECT array_element(['a',NULL,'c','d'], 1);
SELECT array_element(['a',NULL,'c','d'], 2);
SELECT array_element(['a',NULL,'c','d'], 3);
SELECT array_element(['a',NULL,'c','d'], 4);

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 uint64) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4);

SELECT array_element([1,NULL,2,3], col1) FROM test1_00395;

SELECT '----- variable arrays -----';
DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64)));
INSERT INTO test1_00395(col1) VALUES([2,3,7,NULL]);
INSERT INTO test1_00395(col1) VALUES([NULL,3,7,4]);
INSERT INTO test1_00395(col1) VALUES([2,NULL,7,NULL]);
INSERT INTO test1_00395(col1) VALUES([2,3,NULL,4]);
INSERT INTO test1_00395(col1) VALUES([NULL,NULL,NULL,NULL]);
SELECT sleep(3);
SELECT array_element(col1, 1) FROM test1_00395;
SELECT array_element(col1, 2) FROM test1_00395;
SELECT array_element(col1, 3) FROM test1_00395;
SELECT array_element(col1, 4) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(string)));
INSERT INTO test1_00395(col1) VALUES(['a','bc','def',NULL]);
INSERT INTO test1_00395(col1) VALUES([NULL,'bc','def','ghij']);
INSERT INTO test1_00395(col1) VALUES(['a',NULL,'def',NULL]);
INSERT INTO test1_00395(col1) VALUES(['a','bc',NULL,'ghij']);
INSERT INTO test1_00395(col1) VALUES([NULL,NULL,NULL,NULL]);
SELECT sleep(3);
SELECT array_element(col1, 1) FROM test1_00395;
SELECT array_element(col1, 2) FROM test1_00395;
SELECT array_element(col1, 3) FROM test1_00395;
SELECT array_element(col1, 4) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64)), col2 uint64);
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 1);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,3,7,4], 2);
INSERT INTO test1_00395(col1,col2) VALUES([2,NULL,7,NULL], 3);
INSERT INTO test1_00395(col1,col2) VALUES([2,3,NULL,4],4);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,NULL,NULL,NULL],3);
SELECT sleep(3);
SELECT array_element(col1,col2) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(string)), col2 uint64);
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 1);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,'bc','def','ghij'], 2);
INSERT INTO test1_00395(col1,col2) VALUES(['a',NULL,'def','ghij'], 3);
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc',NULL,'ghij'],4);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,NULL,NULL,NULL],3);
SELECT sleep(3);
SELECT array_element(col1,col2) FROM test1_00395;

SELECT '----- has -----';

SELECT '----- constant arrays -----';

SELECT has([1,NULL,2,3], 1);
SELECT has([1,NULL,2,3], NULL);
SELECT has([1,NULL,2,3], 2);
SELECT has([1,NULL,2,3], 3);
SELECT has([1,NULL,2,3], 4);

SELECT has(['a',NULL,'def','ghij'], 'a');
SELECT has(['a',NULL,'def','ghij'], NULL);
SELECT has(['a',NULL,'def','ghij'], 'def');
SELECT has(['a',NULL,'def','ghij'], 'ghij');

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 uint64) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4);
SELECT has([1,NULL,2,3], col1) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint64)) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES(1),(2),(3),(4),(NULL);
SELECT has([1,NULL,2,3], col1) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 string) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES('a'),('bc'),('def'),('ghij');
SELECT has(['a',NULL,'def','ghij'], col1) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(string)) Engine=Memory;
INSERT INTO test1_00395(col1) VALUES('a'),('bc'),('def'),('ghij'),(NULL);
SELECT has(['a',NULL,'def','ghij'], col1) FROM test1_00395;

SELECT '----- variable arrays -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64)));
INSERT INTO test1_00395(col1) VALUES([2,3,7,NULL]);
INSERT INTO test1_00395(col1) VALUES([NULL,3,7,4]);
INSERT INTO test1_00395(col1) VALUES([2,NULL,7,NULL]);
INSERT INTO test1_00395(col1) VALUES([2,3,NULL,4]);
INSERT INTO test1_00395(col1) VALUES([NULL,NULL,NULL,NULL]);
SELECT sleep(3);
SELECT has(col1, 2) FROM test1_00395;
SELECT has(col1, 3) FROM test1_00395;
SELECT has(col1, 4) FROM test1_00395;
SELECT has(col1, 5) FROM test1_00395;
SELECT has(col1, 7) FROM test1_00395;
SELECT has(col1, NULL) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(string)));
INSERT INTO test1_00395(col1) VALUES(['a','bc','def',NULL]);
INSERT INTO test1_00395(col1) VALUES([NULL,'bc','def','ghij']);
INSERT INTO test1_00395(col1) VALUES(['a',NULL,'def',NULL]);
INSERT INTO test1_00395(col1) VALUES(['a','bc',NULL,'ghij']);
INSERT INTO test1_00395(col1) VALUES([NULL,NULL,NULL,NULL]);
SELECT sleep(3);
SELECT has(col1, 'a') FROM test1_00395;
SELECT has(col1, 'bc') FROM test1_00395;
SELECT has(col1, 'def') FROM test1_00395;
SELECT has(col1, 'ghij') FROM test1_00395;
SELECT has(col1,  NULL) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64)), col2 uint64);
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 2);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,3,7,4], 3);
INSERT INTO test1_00395(col1,col2) VALUES([2,NULL,7,NULL], 7);
INSERT INTO test1_00395(col1,col2) VALUES([2,3,NULL,4],5);
SELECT sleep(3);
SELECT has(col1,col2) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint64)), col2 nullable(uint64));
INSERT INTO test1_00395(col1,col2) VALUES([2,3,7,NULL], 2);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,3,7,4], 3);
INSERT INTO test1_00395(col1,col2) VALUES([2,NULL,7,NULL], 7);
INSERT INTO test1_00395(col1,col2) VALUES([2,3,NULL,4],5);
INSERT INTO test1_00395(col1,col2) VALUES([NULL,NULL,NULL,NULL],NULL);
SELECT sleep(3);
SELECT has(col1,col2) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(string)), col2 string);
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 'a');
INSERT INTO test1_00395(col1,col2) VALUES([NULL,'bc','def','ghij'], 'bc');
INSERT INTO test1_00395(col1,col2) VALUES(['a',NULL,'def','ghij'], 'def');
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc',NULL,'ghij'], 'ghij');
SELECT sleep(3);
SELECT has(col1,col2) FROM test1_00395;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(string)), col2 nullable(string));
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc','def',NULL], 'a');
INSERT INTO test1_00395(col1,col2) VALUES([NULL,'bc','def','ghij'], 'bc');
INSERT INTO test1_00395(col1,col2) VALUES(['a',NULL,'def','ghij'], 'def');
INSERT INTO test1_00395(col1,col2) VALUES(['a','bc',NULL,'ghij'], 'ghij');
INSERT INTO test1_00395(col1,col2) VALUES([NULL,NULL,NULL,NULL], NULL);
SELECT sleep(3);
SELECT has(col1,col2) FROM test1_00395;

SELECT '----- Aggregation -----';

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(string), col2 nullable(uint8), col3 string) ;
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'BACDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 1, 'BCADEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 1, 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', 1, 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDEFBGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(NULL, 1, 'ACDEFGBH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(NULL, NULL, 'ACDEFGHB');
SELECT sleep(3);
SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 string, col2 nullable(uint8), col3 string) ;
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 0, 'BACDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 1, 'BCADEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('A', 1, 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('B', 1, 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', 1, 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES('C', NULL, 'ACDEFBGH');
SELECT sleep(3);
SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(string), col2 string) ;
INSERT INTO test1_00395(col1,col2) VALUES('A', 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('A', 'BACDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('A', 'BCADEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('A', 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('B', 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2) VALUES('B', 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2) VALUES('B', 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2) VALUES('B', 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2) VALUES('C', 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('C', 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2) VALUES('C', 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2) VALUES('C', 'ACDEFBGH');
INSERT INTO test1_00395(col1,col2) VALUES(NULL, 'ACDEFGBH');
INSERT INTO test1_00395(col1,col2) VALUES(NULL, 'ACDEFGHB');
SELECT sleep(3);
SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint8), col2 string) ;
INSERT INTO test1_00395(col1,col2) VALUES(0, 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(0, 'BACDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCADEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2) VALUES(1, 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(NULL, 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2) VALUES(NULL, 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2) VALUES(NULL, 'ACDEFBGH');
SELECT sleep(3);
SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint64), col2 uint64, col3 string) ;
INSERT INTO test1_00395(col1,col2,col3) VALUES(0, 2, 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(0, 3, 'BACDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 5, 'BCADEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 2, 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 3, 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 5, 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 2, 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 3, 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2,col3) VALUES(1, 5, 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(NULL, 2, 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(NULL, 3, 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2,col3) VALUES(NULL, 3, 'ACDEFBGH');
SELECT sleep(3);
SELECT col1, col2, count() FROM test1_00395 GROUP BY col1, col2 ORDER BY col1, col2;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 nullable(uint64), col2 uint64, col3 nullable(uint64), col4 string) ;
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(0, 2, 1, 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(0, 3, NULL, 'BACDEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 5, 1, 'BCADEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 2, NULL, 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 3, 1, 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 5, NULL, 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 2, 1, 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 3, NULL, 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(1, 5, 1, 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(NULL, 2, NULL, 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(NULL, 3, 1, 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2,col3,col4) VALUES(NULL, 3, NULL, 'ACDEFBGH');
SELECT sleep(3);
SELECT col1, col2, col3, count() FROM test1_00395 GROUP BY col1, col2, col3 ORDER BY col1, col2, col3;

DROP STREAM IF EXISTS test1_00395;
create stream test1_00395(col1 array(nullable(uint8)), col2 string) ;
INSERT INTO test1_00395(col1,col2) VALUES([0], 'ABCDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([0], 'BACDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCADEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCDAEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCDEAFGH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCDEFAGH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCDEFGAH');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'BCDEFGHA');
INSERT INTO test1_00395(col1,col2) VALUES([1], 'ACBDEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([NULL], 'ACDBEFGH');
INSERT INTO test1_00395(col1,col2) VALUES([NULL], 'ACDEBFGH');
INSERT INTO test1_00395(col1,col2) VALUES([NULL], 'ACDEFBGH');
SELECT sleep(3);
SELECT col1, count() FROM test1_00395 GROUP BY col1 ORDER BY col1;

DROP STREAM IF EXISTS test1_00395;
DROP STREAM test2;
