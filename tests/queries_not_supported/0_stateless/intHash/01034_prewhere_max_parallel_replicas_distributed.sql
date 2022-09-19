-- Tags: replica, distributed
SET query_mode = 'table';
drop stream if exists test_max_parallel_replicas_lr;

-- If you wonder why the table is named with "_lr" suffix in this test.
-- No reason. Actually it is the name of the table in Yandex.Market and they provided this test case for us.

create stream test_max_parallel_replicas_lr (timestamp uint64) ENGINE = MergeTree ORDER BY (intHash32(timestamp)) SAMPLE BY intHash32(timestamp);
INSERT INTO test_max_parallel_replicas_lr select number as timestamp from system.numbers limit 100;

SET max_parallel_replicas = 2;
select count() FROM remote('127.0.0.{2|3}', currentDatabase(), test_max_parallel_replicas_lr) PREWHERE timestamp > 0;

drop stream test_max_parallel_replicas_lr;
