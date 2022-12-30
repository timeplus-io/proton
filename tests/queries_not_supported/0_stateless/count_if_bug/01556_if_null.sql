SELECT
    sumMapIf([1], [1], null_if(number, 3) > 0) AS col1,
    count_if(1, null_if(number, 3) > 0) AS col2,
    sum_if(1, null_if(number, 3) > 0) AS col3
FROM numbers(1, 5);
