select array_filter(x -> 2 * x > 0, []);
select array_filter(x -> 2 * x > 0, [NULL]);
select array_filter(x -> x % 2 ? NULL : 1, [1, 2, 3, 4]);
select array_filter(x -> x % 2, [1, NULL, 3, NULL]);
