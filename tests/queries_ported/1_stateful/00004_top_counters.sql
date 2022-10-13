SELECT CounterID, count() AS c from table(test.hits) GROUP BY CounterID ORDER BY c DESC LIMIT 10;
SELECT CounterID, count() AS c from table(test.hits) GROUP BY CounterID ORDER BY c DESC LIMIT 10 SETTINGS optimize_aggregation_in_order = 1
