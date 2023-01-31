-- Tags: no-fasttest

SELECT DISTINCT json_extract_raw(concat('{"x":', rand() % 2 ? 'true' : 'false', '}'), 'x') AS res FROM numbers(1000000) ORDER BY res;
