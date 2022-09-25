SELECT k, finalize_aggregation(quantiles_timing_state(0.5)(x)) FROM (SELECT int_div(number, 30000 AS d) AS k, number % d AS x FROM system.numbers LIMIT 100000) GROUP BY k WITH TOTALS ORDER BY k;
