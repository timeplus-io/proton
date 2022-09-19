DROP STREAM IF EXISTS mem_test;

create stream mem_test
(
    `a` int64,
    `b` int64
)
;

SET max_block_size = 3;

INSERT INTO mem_test SELECT
    number,
    number
FROM numbers(100);

ALTER STREAM mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER STREAM mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER STREAM mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER STREAM mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER STREAM mem_test
    UPDATE a = 0 WHERE b = 99;

SELECT *
FROM mem_test
FORMAT Null;

DROP STREAM mem_test;
