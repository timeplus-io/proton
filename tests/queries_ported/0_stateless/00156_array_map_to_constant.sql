SELECT array_map(x -> 1, [2]), 123 AS y;
SELECT array_map(x -> x + 1, [2]), 123 AS y;
SELECT array_map(x -> 1, [2, 3]), 123 AS y;
SELECT array_map(x -> x + 1, [2, 3]), 123 AS y;
