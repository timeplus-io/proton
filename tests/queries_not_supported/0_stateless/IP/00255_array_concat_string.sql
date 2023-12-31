SELECT array_string_concat(['Hello', 'World']);
SELECT array_string_concat(materialize(['Hello', 'World']));
SELECT array_string_concat(['Hello', 'World'], ', ');
SELECT array_string_concat(materialize(['Hello', 'World']), ', ');
SELECT array_string_concat(empty_array_string());
SELECT array_string_concat(array_map(x -> to_string(x), range(number))) FROM system.numbers LIMIT 10;
SELECT array_string_concat(array_map(x -> to_string(x), range(number)), '') FROM system.numbers LIMIT 10;
SELECT array_string_concat(array_map(x -> to_string(x), range(number)), ',') FROM system.numbers LIMIT 10;
SELECT array_string_concat(array_map(x -> transform(x, [0, 1, 2, 3, 4, 5, 6, 7, 8], ['yandex', 'google', 'test', '123', '', 'hello', 'world', 'goodbye', 'xyz'], ''), array_map(x -> x % 9, range(number))), ' ') FROM system.numbers LIMIT 20;
SELECT array_string_concat(array_map(x -> to_string(x), range(number % 4))) FROM system.numbers LIMIT 10;
SELECT array_string_concat([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');
SELECT array_string_concat([Null::Nullable(string), Null::Nullable(string)], ';');
SELECT array_string_concat(arr, ';') FROM (SELECT [1, 23, 456] AS arr);
SELECT array_string_concat(arr, ';') FROM (SELECT [Null, 1, Null, 23, Null, 456, Null] AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT [toIPv4('127.0.0.1'), toIPv4('1.0.0.1')] AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT [toIPv4('127.0.0.1'), Null, toIPv4('1.0.0.1')] AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT [to_date('2021-10-01'), to_date('2021-10-02')] AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT [to_date('2021-10-01'), Null, to_date('2021-10-02')] AS arr);
SELECT array_string_concat(materialize([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null]), ';');
SELECT array_string_concat(materialize([Null::Nullable(string), Null::Nullable(string)]), ';');
SELECT array_string_concat(arr, ';') FROM (SELECT materialize([1, 23, 456]) AS arr);
SELECT array_string_concat(arr, ';') FROM (SELECT materialize([Null, 1, Null, 23, Null, 456, Null]) AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT materialize([toIPv4('127.0.0.1'), toIPv4('1.0.0.1')]) AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT materialize([toIPv4('127.0.0.1'), Null, toIPv4('1.0.0.1')]) AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT materialize([to_date('2021-10-01'), to_date('2021-10-02')]) AS arr);
SELECT array_string_concat(arr, '; ') FROM (SELECT materialize([to_date('2021-10-01'), Null, to_date('2021-10-02')]) AS arr);
