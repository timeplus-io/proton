set aggregate_functions_null_for_empty=0;

SELECT quantiles(x, 0.9) FROM (SELECT 1 as x WHERE 0);
SELECT quantiles(number, 0.95) FROM (SELECT number FROM numbers(10) WHERE number > 10);

set aggregate_functions_null_for_empty=1;

SELECT quantiles(x, 0.95) FROM (SELECT 1 as x WHERE 0);
SELECT quantiles(number, 0.95) FROM (SELECT number FROM numbers(10) WHERE number > 10);