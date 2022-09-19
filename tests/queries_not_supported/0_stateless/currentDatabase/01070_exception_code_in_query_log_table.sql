DROP STREAM IF EXISTS test_table_for_01070_exception_code_in_query_log_table;
SELECT * FROM test_table_for_01070_exception_code_in_query_log_table; -- { serverError 60 }
create stream test_table_for_01070_exception_code_in_query_log_table (value uint64) ENGINE=Memory();
SELECT * FROM test_table_for_01070_exception_code_in_query_log_table;
SYSTEM FLUSH LOGS;
SELECT exception_code FROM system.query_log WHERE current_database = currentDatabase() AND lower(query) LIKE lower('SELECT * FROM test_table_for_01070_exception_code_in_query_log_table%') AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE ORDER BY exception_code;
DROP STREAM IF EXISTS test_table_for_01070_exception_code_in_query_log_table;
