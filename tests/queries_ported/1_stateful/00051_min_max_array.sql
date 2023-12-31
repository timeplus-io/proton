SELECT CounterID, count(), max(GoalsReached), min(GoalsReached), min_if(GoalsReached, not_empty(GoalsReached)) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20;
SELECT CounterID, count(), max(GoalsReached), min(GoalsReached), min_if(GoalsReached, not_empty(GoalsReached)) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20 SETTINGS optimize_aggregation_in_order = 1
