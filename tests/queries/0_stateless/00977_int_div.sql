SELECT
    sum(ASD) AS asd,
    int_div(to_int64(asd), abs(to_int64(asd))) AS int_div_with_abs,
    int_div(to_int64(asd), to_int64(asd)) AS int_div_without_abs
FROM
(
    SELECT ASD
    FROM
    (
        SELECT [-1000, -1000] AS asds
    )
    ARRAY JOIN asds AS ASD
);

SELECT  int_div_or_zero( CAST(-1000, 'int64')   , CAST(1000, 'uint64') );
SELECT  int_div_or_zero( CAST(-1000, 'int64')   , CAST(1000, 'int64') );

SELECT int_div(-1, number) FROM numbers(1, 10);
SELECT int_div_or_zero(-1, number) FROM numbers(1, 10);
SELECT int_div(to_int32(number), -1) FROM numbers(1, 10);
SELECT int_div_or_zero(to_int32(number), -1) FROM numbers(1, 10);
SELECT int_div(to_int64(number), -1) FROM numbers(1, 10);
SELECT int_div_or_zero(to_int64(number), -1) FROM numbers(1, 10);
SELECT int_div(number, -number) FROM numbers(1, 10);
SELECT int_div_or_zero(number, -number) FROM numbers(1, 10);

SELECT -1 DIV number FROM numbers(1, 10);
SELECT to_int32(number) DIV -1 FROM numbers(1, 10);
SELECT to_int64(number) DIV -1 FROM numbers(1, 10);
SELECT number DIV -number FROM numbers(1, 10);
SELECT -1 DIV 0;  -- { serverError 153 }
