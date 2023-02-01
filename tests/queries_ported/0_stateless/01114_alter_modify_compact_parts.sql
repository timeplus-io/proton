DROP STREAM IF EXISTS mt_compact;

CREATE STREAM mt_compact (d Date, id uint32, s string)
    ENGINE = MergeTree ORDER BY id PARTITION BY d
    SETTINGS min_bytes_for_wide_part = 10000000, index_granularity = 128;

INSERT INTO mt_compact SELECT to_date('2020-01-05'), number, to_string(number) FROM numbers(1000);
INSERT INTO mt_compact SELECT to_date('2020-01-06'), number, to_string(number) FROM numbers(1000);
ALTER STREAM mt_compact MODIFY COLUMN s uint64;
SELECT sum(s) from mt_compact;

DROP STREAM IF EXISTS mt_compact;
