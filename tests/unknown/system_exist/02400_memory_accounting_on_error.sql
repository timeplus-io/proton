-- max_block_size to avoid randomization
SELECT * FROM generateRandom('i array(int8)', 1, 1, 1048577) LIMIT 65536 SETTINGS max_memory_usage='1Gi', max_block_size=65505, log_queries=1; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM FLUSH LOGS;
SELECT * FROM system.query_log WHERE event_date >= yesterday() AND current_database = current_database() AND memory_usage > 100e6 FORMAT JSONEachRow;
