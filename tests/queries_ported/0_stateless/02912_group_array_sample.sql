-- Checks that the random seed is different for multiple states of aggregation:
SELECT uniq(x) > 50 FROM (SELECT number, group_array_sample(array_join(range(1000)), 10) AS x FROM numbers(100) GROUP BY number);
