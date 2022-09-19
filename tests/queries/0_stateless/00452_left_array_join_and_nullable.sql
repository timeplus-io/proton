SELECT emptyArrayToSingle(array_map(x -> nullIf(x, 2), array_join([empty_array_uint8(), [1], [2, 3]]))) AS arr;
SELECT arr, element FROM (SELECT array_map(x -> nullIf(x, 2), array_join([empty_array_uint8(), [1], [2, 3]])) AS arr) LEFT ARRAY JOIN arr AS element;

SELECT emptyArrayToSingle(arr) FROM (SELECT array_map(x -> (x, to_string(x), x = 1 ? NULL : x), range(number % 3)) AS arr FROM system.numbers LIMIT 10);

SELECT emptyArrayToSingle(array_map(x -> to_string(x), array_map(x -> nullIf(x, 2), array_join([empty_array_uint8(), [1], [2, 3]])))) AS arr;
SELECT emptyArrayToSingle(array_map(x -> to_fixed_string(to_string(x), 3), array_map(x -> nullIf(x, 2), array_join([empty_array_uint8(), [1], [2, 3], [3, 4, 5]])))) AS arr;
