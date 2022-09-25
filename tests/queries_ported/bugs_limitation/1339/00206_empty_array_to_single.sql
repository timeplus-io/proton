SELECT empty_array_to_single(array_filter(x -> x != 99, array_join([[1, 2], [99], [4, 5, 6]])));
SELECT empty_array_to_single(empty_array_string()), empty_array_to_single(empty_array_date()), empty_array_to_single(array_filter(x -> false, [now('Europe/Moscow')]));

SELECT
    empty_array_to_single(range(number % 3)),
    empty_array_to_single(array_map(x -> to_string(x), range(number % 2))),
    empty_array_to_single(array_map(x -> to_datetime('2015-01-01 00:00:00', 'UTC') + x, range(number % 5))),
    empty_array_to_single(array_map(x -> to_date('2015-01-01') + x, range(number % 4))) FROM system.numbers LIMIT 10;
