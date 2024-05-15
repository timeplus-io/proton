with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.9), p90(f) from cte;

with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.95), p95(f) from cte;

with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.99), p99(f) from cte;

SELECT quantile_exact_if(number, number > 0) FROM numbers(90);

SELECT quantile_exact_if(number, number > 100) FROM numbers(90);
SELECT quantile_exact_if(to_float32(number) , number > 100) FROM numbers(90);
SELECT quantile_exact_if(to_float64(number) , number > 100) FROM numbers(90);

SELECT quantile_if(number, number > 100) FROM numbers(90);
SELECT quantile_if(to_float32(number) , number > 100) FROM numbers(90);
SELECT quantile_if(to_float64(number) , number > 100) FROM numbers(90);

SELECT quantiles(x, 0.5) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_exact(x, 0.5) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_t_digest(x, 0.5) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT quantiles_deterministic(x, x, 0.5) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT quantiles(x, 0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_exact(x, 0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_t_digest(x, 0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_deterministic(x, x, 0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999, 1) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT quantiles_exact(x, 1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_exact_weighted(x, 1, 1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_timing(x, 1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles(x, 1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);
SELECT quantiles_deterministic(x, x, 1, 0.001, 0.01, 0.05, 0.9, 0.2, 0.3, 0.6, 0.5, 0.4, 0.7, 0.8, 0.1, 0.95, 0.99, 0.999, 0, 0.5, 0.3, 0.4) FROM (SELECT number AS x FROM system.numbers LIMIT 1001);

SELECT quantile_exact_weighted(x, 1, 0.5) AS q5, quantiles_exact_weighted(x, 1, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1) AS qs FROM (SELECT array_join([1, 1, 1, 10, 10, 10, 10, 100, 100, 100]) AS x);
SELECT quantile_exact(x, 0), quantile_timing(x, 0) FROM (SELECT number + 100 AS x FROM system.numbers LIMIT 10000);
SELECT quantile_exact(x), quantile_timing(x) FROM (SELECT number % 123 AS x FROM system.numbers LIMIT 10000);