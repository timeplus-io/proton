SELECT 'Hello, "World"' AS x, 123 AS y, [1, 2, 3] AS z, (456, ['abc', 'def']) AS a, 'Newline\nhere' AS b FORMAT CSV;
SELECT 'Hello, "World"' AS x, 123 AS y, [1, 2, 3] AS z, (456, ['abc', 'def']) AS a, 'Newline\nhere' AS b FORMAT CSVWithNames;
SELECT 'Hello, "World"' AS x, 123 AS y, [1, 2, 3] AS z, (456, ['abc', 'def']) AS a, 'Newline\nhere' AS b FORMAT CSVWithNamesAndTypes;
SELECT number, to_string(number), range(number), to_date('2000-01-01') + number, to_datetime('2000-01-01 00:00:00') + number FROM system.numbers LIMIT 10 FORMAT CSV;
