DROP STREAM IF EXISTS agg;

CREATE STREAM agg
(
    `key` uint32,
    `ts` DateTime,
    `value` uint32
)
ENGINE = MergeTree
ORDER BY (key, ts);

SET max_bytes_before_external_group_by=1;

INSERT INTO agg(key,ts,value) SELECT 1, to_datetime('2021-12-06 00:00:00') + number, number FROM numbers(100000);

DROP STREAM agg;
