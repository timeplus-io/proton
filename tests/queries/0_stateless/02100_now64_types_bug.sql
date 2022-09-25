SELECT x
FROM
(
    SELECT if((number % NULL) = -2147483648, NULL, if(to_int64(to_int64(now64(if((number % NULL) = -2147483648, NULL, if(to_int64(now64(to_int64(9223372036854775807, now64(plus(NULL, NULL))), plus(NULL, NULL))) = (number % NULL), nan, to_float64(number))), to_int64(9223372036854775807, to_int64(9223372036854775807, now64(plus(NULL, NULL))), now64(plus(NULL, NULL))), plus(NULL, NULL))), now64(to_int64(9223372036854775807, to_int64(0, now64(plus(NULL, NULL))), now64(plus(NULL, NULL))), plus(NULL, NULL))) = (number % NULL), nan, to_float64(number))) AS x
    FROM system.numbers
    LIMIT 3
)
ORDER BY x DESC NULLS LAST
