SET allow_experimental_analyzer=1;

SELECT cast(tuple(1, 2), 'tuple(value_1 uint64, value_2 uint64)') AS value, value.value_1, value.value_2;

SELECT '--';

SELECT value.value_1, value.value_2 FROM (SELECT cast(tuple(1, 2), 'tuple(value_1 uint64, value_2 uint64)') AS value);
