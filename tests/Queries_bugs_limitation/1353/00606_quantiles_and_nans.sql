SELECT DISTINCT
    eq
FROM
    (
        WITH
            range(2 + number % 10) AS arr, -- minimum two elements, to avoid nan result --
            array_map(x -> x = int_div(number, 10) ? nan : x, arr) AS arr_with_nan,
            array_filter(x -> x != int_div(number, 10), arr) AS arr_filtered
        SELECT
            number,
            array_reduce('quantileExact', arr_with_nan) AS q1,
            array_reduce('quantileExact', arr_filtered) AS q2,
            q1 = q2 AS eq
        FROM
            numbers(100)
    );
