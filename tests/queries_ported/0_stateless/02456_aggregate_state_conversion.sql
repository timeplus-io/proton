SELECT hex(CAST(x, 'aggregate_function(sum, decimal(50, 10))')) FROM (SELECT array_reduce('sum_state', [to_decimal256('0.0000010.000001', 10)]) AS x) GROUP BY x;
