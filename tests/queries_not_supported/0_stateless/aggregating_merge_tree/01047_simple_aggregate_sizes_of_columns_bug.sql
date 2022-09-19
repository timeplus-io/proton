DROP STREAM IF EXISTS column_size_bug;

create stream column_size_bug (date_time DateTime, value SimpleAggregateFunction(sum,uint64)) ENGINE = AggregatingMergeTree PARTITION BY to_start_of_interval(date_time, INTERVAL 1 DAY) ORDER BY (date_time) SETTINGS remove_empty_parts = 0;

INSERT INTO column_size_bug VALUES(now(),1);
INSERT INTO column_size_bug VALUES(now(),1);

ALTER STREAM column_size_bug DELETE WHERE value=1;

-- wait for DELETE
SELECT sleep(1);

OPTIMIZE STREAM column_size_bug;

DROP STREAM column_size_bug;
