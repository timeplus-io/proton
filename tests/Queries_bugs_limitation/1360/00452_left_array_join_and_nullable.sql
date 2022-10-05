SELECT empty_array_to_single(array_map(x -> null_if(x, 2), array_join([empty_array_uint8(), [1], [2, 3]]))) AS arr;
SELECT arr, element FROM (SELECT array_map(x -> null_if(x, 2), array_join([empty_array_uint8(), [1], [2, 3]])) AS arr) LEFT ARRAY JOIN arr AS element;

SELECT empty_array_to_single(arr) FROM (SELECT array_map(x -> (x, to_string(x), x = 1 ? NULL : x), range(number % 3)) AS arr FROM system.numbers LIMIT 10);

SELECT empty_array_to_single(array_map(x -> to_string(x), array_map(x -> null_if(x, 2), array_join([empty_array_uint8(), [1], [2, 3]])))) AS arr;
SELECT empty_array_to_single(array_map(x -> to_fixed_string(to_string(x), 3), array_map(x -> null_if(x, 2), array_join([empty_array_uint8(), [1], [2, 3], [3, 4, 5]])))) AS arr;
