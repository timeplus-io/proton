SELECT bitmapMax(arg_max(x, y))
FROM remote('127.0.0.{2,3}', view(
    SELECT
        groupBitmapState(to_uint32(number)) AS x,
        number AS y
    FROM numbers(10)
    GROUP BY number
));
