SELECT EventDate, finalize_aggregation(state) FROM (SELECT EventDate, uniq_state(UserID) AS state FROM test.hits GROUP BY EventDate ORDER BY EventDate);
