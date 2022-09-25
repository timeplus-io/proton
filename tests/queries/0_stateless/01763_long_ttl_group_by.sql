-- Tags: long

DROP STREAM IF EXISTS test_ttl_group_by01763;
create stream test_ttl_group_by01763
(key uint32, ts datetime, value uint32, min_value uint32 default value, max_value uint32 default value)
ENGINE = MergeTree() PARTITION BY to_YYYYMM(ts)
ORDER BY (key, to_start_of_interval(ts, toIntervalMinute(3)), ts) 
TTL ts + INTERVAL 5 MINUTE GROUP BY key, to_start_of_interval(ts, toIntervalMinute(3)) 
SET value = sum(value), min_value = min(min_value), max_value = max(max_value),  ts=min(to_start_of_interval(ts, toIntervalMinute(3)));

INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 1 FROM numbers(100000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 0 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 3 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 1 FROM numbers(100000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 0 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 3 FROM numbers(1000);

SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763;
SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763 where key = 3 ;
SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763 where key = 3 and ts <= today() - interval 30 day ;

OPTIMIZE TABLE test_ttl_group_by01763 FINAL;

SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763;
SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763 where key = 3 ;
SELECT sum(value), min(min_value), max(max_value), uniq_exact(key) FROM test_ttl_group_by01763 where key = 3 and ts <= today() - interval 30 day ;

DROP STREAM test_ttl_group_by01763;
