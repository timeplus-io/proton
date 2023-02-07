SET allow_experimental_analyzer = 1;

SELECT number FROM numbers(untuple(tuple(1)));

SELECT '--';

SELECT number FROM numbers(untuple(tuple(0, 2)));

SELECT '--';

SELECT number FROM numbers(untuple(tuple(1, 2)));

SELECT '--';

SELECT cast(tuple(1), 'tuple(value uint64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(0, 1), 'tuple(value_1 uint64, value_2 uint64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(1, 2), 'tuple(value_1 uint64, value_2 uint64)') AS value, number FROM numbers(untuple(value));

SELECT '--';

SELECT cast(tuple(1), 'tuple(value uint64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple(0, 1), 'tuple(value_1 uint64, value_2 uint64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple(1, 2), 'tuple(value_1 uint64, value_2 uint64)') AS value, number FROM numbers(value.*);

SELECT '--';

SELECT cast(tuple('1'), 'tuple(value string)') AS value, number FROM numbers(value.* APPLY x -> to_uint64(x));

SELECT '--';

SELECT cast(tuple('0', '1'), 'tuple(value_1 string, value_2 string)') AS value, number FROM numbers(value.* APPLY x -> to_uint64(x));

SELECT '--';

SELECT cast(tuple('1', '2'), 'tuple(value_1 string, value_2 string)') AS value, number FROM numbers(value.* APPLY x -> to_uint64(x));
