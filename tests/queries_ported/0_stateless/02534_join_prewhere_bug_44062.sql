
DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;

CREATE STREAM test1 ( `col1` uint64, `col2` int8 ) ENGINE = MergeTree ORDER BY col1;
CREATE STREAM test2 ( `col1` uint64, `col3` int16 ) ENGINE = MergeTree ORDER BY col1;

INSERT INTO test1 VALUES (123, 123), (12321, -30), (321, -32);
INSERT INTO test2 VALUES (123, 5600), (321, 5601);

SET join_use_nulls = 1;

-- { echoOn }

SELECT * FROM test1 LEFT JOIN test2 ON test1.col1 = test2.col1
WHERE test2.col1 IS NULL
ORDER BY test2.col1
;
-- proton: starts. FIXME. right join with null is wrong
-- SELECT * FROM test2 RIGHT JOIN test1 ON test2.col1 = test1.col1
-- WHERE test2.col1 IS NULL
-- ORDER BY test2.col1
-- expected： \N	\N	12321	-30
-- proton: ends
-- ;

SELECT * FROM test1 LEFT JOIN test2 ON test1.col1 = test2.col1
WHERE test2.col1 IS NOT NULL
ORDER BY test2.col1
;

SELECT * FROM test2 RIGHT JOIN test1 ON test2.col1 = test1.col1
WHERE test2.col1 IS NOT NULL
ORDER BY test2.col1
;

SELECT test2.col1, test1.* FROM test2 RIGHT JOIN test1 ON test2.col1 = test1.col1
WHERE test2.col1 IS NOT NULL
ORDER BY test2.col1
;

SELECT test2.col3, test1.* FROM test2 RIGHT JOIN test1 ON test2.col1 = test1.col1
WHERE test2.col1 IS NOT NULL
ORDER BY test2.col1
;

DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
