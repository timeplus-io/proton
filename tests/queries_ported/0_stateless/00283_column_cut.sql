SELECT
    number,
    to_string(number),
    range(number) AS arr,
    array_map(x -> to_string(x), arr) AS arr_s,
    array_map(x -> range(x), arr) AS arr_arr,
    array_map(x -> array_map(y -> to_string(y), x), arr_arr) AS arr_arr_s,
    array_map(x -> to_fixed_string(x, 3), arr_s) AS arr_fs
FROM system.numbers
LIMIT 5, 10;
