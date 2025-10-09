DROP STREAM IF EXISTS test_parallel_index;

CREATE STREAM test_parallel_index
(
    z uint64,
    INDEX i z TYPE set(8)
)
ENGINE = MergeTree
ORDER BY ();

insert into test_parallel_index select number from numbers(10);

select sum(z) from test_parallel_index where z = 2 or z = 7 or z = 13 or z = 17 or z = 19 or z = 23;

DROP STREAM test_parallel_index;
