-- Tags: no-parallel

create stream test
(
    `x` tuple(uint64, uint64)
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO test SELECT (number, number) FROM numbers(1000000);

SELECT COUNT() FROM test;

ALTER STREAM test DETACH PARTITION tuple();

ALTER STREAM test ATTACH PARTITION tuple();

SELECT COUNT() FROM test;

DETACH TABLE test;

ATTACH TABLE test;

SELECT COUNT() FROM test;

DROP STREAM test;
