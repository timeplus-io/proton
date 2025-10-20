SET output_format_write_statistics = 0;
set output_format_json_pretty_print = 0;

SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT RowBinary;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT RowBinaryWithNamesAndTypes;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT TabSeparatedWithNamesAndTypes;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT TabSeparatedRaw;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT CSV;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT JSON;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT JSONCompact;
SELECT number * 246 + 10 AS n, to_date32('2000-01-01') + n AS d, range(n) AS arr, array_string_concat(array_map(x -> reinterpret_as_string(x), arr)) AS s, (n, d) AS tuple FROM system.numbers LIMIT 2 FORMAT XML;
