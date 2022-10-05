-- Tags: no-parallel

SELECT
    0 OR NULL,
    1 OR NULL,
    to_nullable(0) OR NULL,
    to_nullable(1) OR NULL,
    0.0 OR NULL,
    0.1 OR NULL,
    NULL OR 1 OR NULL,
    0 OR NULL OR 1 OR NULL;

SELECT
    0 AND NULL,
    1 AND NULL,
    to_nullable(0) AND NULL,
    to_nullable(1) AND NULL,
    0.0 AND NULL,
    0.1 AND NULL,
    NULL AND 1 AND NULL,
    0 AND NULL AND 1 AND NULL;

SELECT
    x,
    0 OR x,
    1 OR x,
    x OR x,
    to_nullable(0) OR x,
    to_nullable(1) OR x,
    0.0 OR x,
    0.1 OR x,
    x OR 1 OR x,
    0 OR x OR 1 OR x
FROM (SELECT (number % 2 <> 0) ? number % 3 : NULL AS x FROM system.numbers LIMIT 10);

SELECT
    x,
    0 AND x,
    1 AND x,
    x AND x,
    to_nullable(0) AND x,
    to_nullable(1) AND x,
    0.0 AND x,
    0.1 AND x,
    x AND 1 AND x,
    0 AND x AND 1 AND x
FROM (SELECT (number % 2 <> 0) ? number % 3 : NULL AS x FROM system.numbers LIMIT 10);

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS test;

create stream test
(
    x nullable(int32)
)  ;

INSERT INTO test(x) VALUES(1), (0), (null);
SELECT sleep(3);
SELECT * FROM test;
SELECT x FROM test WHERE x != 0;
SELECT x FROM test WHERE x != 0 OR is_null(x);
SELECT x FROM test WHERE x != 1;

DROP STREAM test;
