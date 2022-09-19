-- Tags: no-parallel

-- check ALTER MODIFY COLUMN with partitions

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS alter_column;

create stream alter_column(x uint32, y int32) ENGINE MergeTree PARTITION BY x ORDER BY x;
INSERT INTO alter_column (x, y) SELECT number AS x, -number AS y FROM system.numbers LIMIT 50;

SELECT '*** Check SHOW create stream ***';
SHOW create stream alter_column;

SELECT '*** Check parts ***';
SELECT * FROM alter_column ORDER BY _part;

ALTER STREAM alter_column MODIFY COLUMN y int64;

SELECT '*** Check SHOW create stream after ALTER MODIFY ***';
SHOW create stream alter_column;

SELECT '*** Check parts after ALTER MODIFY ***';
SELECT * FROM alter_column ORDER BY _part;

DROP STREAM alter_column;
