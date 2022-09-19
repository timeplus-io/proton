SELECT topK(10)(n) FROM (SELECT if(number % 100 < 10, number % 10, number) AS n FROM system.numbers LIMIT 100000);

SELECT
    k,
    topK(v)
FROM
(
    SELECT
        number % 7 AS k,
        array_map(x -> array_map(x -> if(x = 0, NULL, to_string(x)), range(x)), range(int_div(number, 1))) AS v
    FROM system.numbers
    LIMIT 10
)
GROUP BY k
ORDER BY k ASC
