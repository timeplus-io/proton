SELECT quantileExactWeighted(0.5)(x, 1) AS q5, quantilesExactWeighted(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)(x, 1) AS qs FROM (SELECT array_join([1, 1, 1, 10, 10, 10, 10, 100, 100, 100]) AS x);
SELECT quantileExact(0)(x), quantile_timing(0)(x) FROM (SELECT number + 100 AS x FROM system.numbers LIMIT 10000);
SELECT quantileExact(x), quantile_timing(x) FROM (SELECT number % 123 AS x FROM system.numbers LIMIT 10000);
