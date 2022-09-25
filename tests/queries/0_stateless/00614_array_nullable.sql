SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test;
create stream test(date date, keys array(nullable(uint8))) ENGINE = MergeTree(date, date, 1);
INSERT INTO(date,keys) test VALUES ('2017-09-10', [1, 2, 3, 4, 5, 6, 7, NULL]);
SELECT sleep(3);

SELECT * FROM test LIMIT 1;
SELECT avgArray(keys) FROM test;
DROP STREAM test;
