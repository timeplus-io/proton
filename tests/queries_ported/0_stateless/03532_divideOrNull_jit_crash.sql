-- https://github.com/ClickHouse/ClickHouse/issues/81346
SELECT (NOT divide_or_null(0, *)) AND (NOT int_div_or_null(*, 1)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
