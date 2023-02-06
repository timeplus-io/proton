DROP STREAM IF EXISTS ev;
DROP STREAM IF EXISTS idx;

CREATE STREAM ev (a int32, b int32) Engine=MergeTree() ORDER BY a;
CREATE STREAM idx (a int32) Engine=MergeTree() ORDER BY a;
INSERT INTO ev SELECT number, number FROM numbers(10000000);
INSERT INTO idx SELECT number * 5 FROM numbers(1000);

-- test_enable_global_with_statement_performance_1
WITH 'test' AS u SELECT count() FROM ev WHERE a IN (SELECT a FROM idx) SETTINGS enable_global_with_statement = 1;

-- test_enable_global_with_statement_performance_2
SELECT count() FROM ev WHERE a IN (SELECT a FROM idx) SETTINGS enable_global_with_statement = 1;

-- test_enable_global_with_statement_performance_3
WITH 'test' AS u SELECT count() FROM ev WHERE a IN (SELECT a FROM idx) SETTINGS enable_global_with_statement = 0;

SYSTEM FLUSH LOGS;

SELECT count(read_rows) FROM (SELECT read_rows FROM system.query_log WHERE current_database=current_database() AND type='QueryFinish' AND query LIKE '-- test_enable_global_with_statement_performance%' ORDER BY initial_query_start_time_microseconds DESC LIMIT 3) GROUP BY read_rows;

DROP STREAM IF EXISTS ev;
DROP STREAM IF EXISTS idx;
