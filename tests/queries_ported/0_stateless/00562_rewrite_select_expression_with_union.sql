SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS test_00562;

create stream test_00562 ( s string,  i int64) ;

INSERT INTO test_00562(s,i) VALUES('test_string', 1);
SELECT sleep(3);
SELECT s, SUM(i*2) AS i FROM test_00562 GROUP BY s  UNION ALL  SELECT s, SUM(i*2) AS i FROM test_00562 GROUP BY s;
SELECT s FROM (SELECT s, SUM(i*2) AS i FROM test_00562 GROUP BY s  UNION ALL  SELECT s, SUM(i*2) AS i FROM test_00562 GROUP BY s);

DROP STREAM IF EXISTS test_00562;
