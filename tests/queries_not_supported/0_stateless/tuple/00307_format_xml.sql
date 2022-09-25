SET output_format_write_statistics = 0;
SELECT 'Hello & world' AS s, 'Hello\n<World>', to_datetime('2001-02-03 04:05:06') AS time, array_map(x -> to_string(x), range(10)) AS arr, (s, time) AS tpl SETTINGS extremes = 1 FORMAT XML;
