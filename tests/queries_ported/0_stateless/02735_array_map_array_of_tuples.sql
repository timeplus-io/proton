SELECT array_map((x) -> x, [tuple_cast(1)]);
SELECT array_map((x) -> x.1, [tuple_cast(1)]);
SELECT array_map((x) -> x.1 + x.2, [tuple_cast(1, 2)]);
SELECT array_map((x, y) -> x + y, [tuple_cast(1, 2)]);
