SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test_00209;
create stream test_00209 (x uint8)  ;

INSERT INTO test_00209(x)  SELECT 1 AS x;
INSERT INTO test_00209(x) SELECT 1 AS x SETTINGS extremes = 1;
INSERT INTO test_00209(x)  SELECT 1 AS x GROUP BY 1 WITH TOTALS;
INSERT INTO test_00209(x)  SELECT 1 AS x GROUP BY 1 WITH TOTALS SETTINGS extremes = 1;

SELECT sleep(3);

SELECT count(), min(x), max(x) FROM test_00209;

DROP STREAM test_00209;
