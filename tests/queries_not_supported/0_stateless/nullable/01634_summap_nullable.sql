SELECT sum_map(['a', 'b'], [1, NULL]);
SELECT sum_map(['a', 'b'], [1, to_nullable(0)]);
