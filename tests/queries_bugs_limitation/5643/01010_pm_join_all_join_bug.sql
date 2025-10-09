SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS ints;
CREATE STREAM ints (i64 int64, i32 int32) ENGINE = Memory;

SET join_algorithm = 'partial_merge';

INSERT INTO ints SELECT 1 AS i64, number AS i32 FROM numbers(2);

SELECT * FROM ints as l LEFT JOIN ints as r USING i64 ORDER BY l.i32, r.i32;
SELECT '-';
SELECT * FROM ints as l INNER JOIN ints as r USING i64 ORDER BY l.i32, r.i32;

SELECT '-';
SELECT count() FROM ( SELECT [1], count(1) ) AS t1 ALL RIGHT JOIN ( SELECT number AS s FROM numbers(2) ) AS t2 USING (s); -- { serverError UNKNOWN_IDENTIFIER }

DROP STREAM ints;