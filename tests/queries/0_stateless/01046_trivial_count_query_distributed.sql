-- Tags: distributed

DROP STREAM IF EXISTS test_count;

create stream test_count (`pt` date) ENGINE = MergeTree PARTITION BY pt ORDER BY pt SETTINGS index_granularity = 8192;

INSERT INTO test_count values ('2019-12-12');

SELECT count(1) FROM remote('127.0.0.{1,1,2}', currentDatabase(), test_count);

DROP STREAM test_count;
