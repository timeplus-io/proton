SELECT EventDate, finalize_aggregation(state), runningAccumulate(state) FROM (SELECT EventDate, uniq_state(UserID) AS state FROM table(test.hits) GROUP BY EventDate ORDER BY EventDate);
