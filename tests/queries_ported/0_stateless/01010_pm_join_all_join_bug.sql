set query_mode='table';
set asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS ints;
create stream ints (i64 int64, i32 int32) ;

SET join_algorithm = 'partial_merge';

INSERT INTO ints(i64,i32) SELECT 1 AS i64, number AS i32 FROM numbers(2);
SELECT sleep(3);

SELECT * FROM ints l LEFT JOIN ints r USING i64 ORDER BY l.i32, r.i32;
SELECT '-';
SELECT * FROM ints l INNER JOIN ints r USING i64 ORDER BY l.i32, r.i32;

SELECT '-';
SELECT count() FROM ( SELECT [1], count(1) ) AS t1 ALL RIGHT JOIN ( SELECT number AS s FROM numbers(2) ) AS t2 USING (s); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP stream ints;
