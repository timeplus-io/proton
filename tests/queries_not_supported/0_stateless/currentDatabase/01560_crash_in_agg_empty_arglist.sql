-- make sure the system.query_log table is created
SELECT 1;
SYSTEM FLUSH LOGS;

SELECT any() as t, substring(query, 1, 70) AS query, avg(memory_usage) usage, count() count FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= to_date(1604295323) AND event_time >= to_datetime(1604295323) AND type in (1,2,3,4) and initial_user in ('') and('all' = 'all' or(position_case_insensitive(query, 'all') = 1)) GROUP BY query ORDER BY usage desc LIMIT 5; -- { serverError 42 }
