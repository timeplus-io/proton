SELECT number % 100 AS k, sum_array(empty_array_uint8()) AS v FROM numbers(10) GROUP BY k;
