SELECT emptyArrayToSingle(array_filter(x -> x != 99, array_join([[1, 2], [99], [4, 5, 6]])));
SELECT emptyArrayToSingle(empty_array_string()), emptyArrayToSingle(emptyArrayDate()), emptyArrayToSingle(array_filter(x -> 0, [now('Europe/Moscow')]));

SELECT
    emptyArrayToSingle(range(number % 3)),
    emptyArrayToSingle(array_map(x -> to_string(x), range(number % 2))),
    emptyArrayToSingle(array_map(x -> to_datetime('2015-01-01 00:00:00', 'UTC') + x, range(number % 5))),
    emptyArrayToSingle(array_map(x -> to_date('2015-01-01') + x, range(number % 4))) FROM system.numbers LIMIT 10;
