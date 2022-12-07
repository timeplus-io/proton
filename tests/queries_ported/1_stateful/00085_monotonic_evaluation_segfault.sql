SELECT any(0) FROM test.visits WHERE (to_int32(to_datetime(StartDate))) > 1000000000;
