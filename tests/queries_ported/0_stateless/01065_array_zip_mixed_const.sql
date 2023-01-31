SELECT array_zip([0, 1], ['hello', 'world']);
SELECT array_zip(materialize([0, 1]), ['hello', 'world']);
SELECT array_zip([0, 1], materialize(['hello', 'world']));
SELECT array_zip(materialize([0, 1]), materialize(['hello', 'world']));

SELECT array_zip([0, number], [to_string(number), 'world']) FROM numbers(10);
SELECT array_zip([1, number, number * number], [[], [], []]) FROM numbers(10);
