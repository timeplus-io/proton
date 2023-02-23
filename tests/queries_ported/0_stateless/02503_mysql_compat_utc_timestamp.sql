-- PowerBI is doing this query. It should work at least somehow, not necessarily in the same way as in MySQL.
SELECT time_diff(NOW(), UTC_TIMESTAMP()) DIV 600;
