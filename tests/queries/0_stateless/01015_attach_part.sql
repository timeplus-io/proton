-- Tags: no-parallel

DROP STREAM IF EXISTS table_01;

create stream table_01 (
    date date,
    n int32
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY date;

INSERT INTO table_01 SELECT to_date('2019-10-01'), number FROM system.numbers LIMIT 1000;

SELECT count() FROM table_01;

ALTER STREAM table_01 DETACH PARTITION ID '20191001';

SELECT count() FROM table_01;

ALTER STREAM table_01 ATTACH PART '20191001_1_1_0';

SELECT count() FROM table_01;

DROP STREAM IF EXISTS table_01;
