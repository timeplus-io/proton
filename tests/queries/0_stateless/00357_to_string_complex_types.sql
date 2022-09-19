SELECT to_string((1, 'Hello', to_date('2016-01-01'))), to_string([1, 2, 3]);
SELECT (number, to_string(number), range(number)) AS x, to_string(x) FROM system.numbers LIMIT 10;
SELECT hex(to_string(countState())) FROM (SELECT * FROM system.numbers LIMIT 10);

SELECT CAST((1, 'Hello', to_date('2016-01-01')) AS string), CAST([1, 2, 3] AS string);
SELECT (number, to_string(number), range(number)) AS x, CAST(x AS string) FROM system.numbers LIMIT 10;
SELECT hex(CAST(countState() AS string)) FROM (SELECT * FROM system.numbers LIMIT 10);
