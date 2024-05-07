with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.9), p90(f) from cte;

with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.95), p95(f) from cte;

with cte as (select array_join([100, 200, 300, 400, 500]) as f) select quantile(f, 0.99), p99(f) from cte;
