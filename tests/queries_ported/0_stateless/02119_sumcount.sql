-- Integer types are added as integers
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::uint64 AS v
        UNION ALL
        SELECT '1'::uint64 AS v
        UNION ALL SELECT '1'::uint64 AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::nullable(uint64) AS v
        UNION ALL
        SELECT '1'::nullable(uint64) AS v
        UNION ALL
        SELECT '1'::nullable(uint64) AS v
    )
    ORDER BY v
);

SET allow_suspicious_low_cardinality_types=1;

SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
	(
        SELECT '9007199254740992'::low_cardinality(uint64) AS v
        UNION ALL
        SELECT '1'::low_cardinality(uint64) AS v
        UNION ALL
        SELECT '1'::low_cardinality(uint64) AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::low_cardinality(nullable(uint64)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(uint64)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(uint64)) AS v
    )
    ORDER BY v
);

-- -- float64 types are added as float64
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::float64 AS v
        UNION ALL
        SELECT '1'::float64 AS v
        UNION ALL SELECT '1'::float64 AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::nullable(float64) AS v
        UNION ALL
        SELECT '1'::nullable(float64) AS v
        UNION ALL
        SELECT '1'::nullable(float64) AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::low_cardinality(float64) AS v
        UNION ALL
        SELECT '1'::low_cardinality(float64) AS v
        UNION ALL
        SELECT '1'::low_cardinality(float64) AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '9007199254740992'::low_cardinality(nullable(float64)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(float64)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(float64)) AS v
    )
    ORDER BY v
);

-- -- float32 are added as float64
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::float32 AS v
        UNION ALL
        SELECT '1'::float32 AS v
        UNION ALL
        SELECT '1'::float32 AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::nullable(float32) AS v
        UNION ALL
        SELECT '1'::nullable(float32) AS v
        UNION ALL
        SELECT '1'::nullable(float32) AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::low_cardinality(float32) AS v
        UNION ALL
        SELECT '1'::low_cardinality(float32) AS v
        UNION ALL
        SELECT '1'::low_cardinality(float32) AS v
    )
    ORDER BY v
);
SELECT to_type_name(sum_count(v)), sum_count(v) FROM
(
    SELECT v FROM
    (
        SELECT '16777216'::low_cardinality(nullable(float32)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(float32)) AS v
        UNION ALL
        SELECT '1'::low_cardinality(nullable(float32)) AS v
    )
    ORDER BY v
);

-- Small integer types use their sign/unsigned 64 byte supertype
SELECT to_type_name(sum_count(number::int8)), sum_count(number::int8) FROM numbers(120);
SELECT to_type_name(sum_count(number::uint8)), sum_count(number::uint8) FROM numbers(250);

-- Greater integers use their own type
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT '1'::int128 AS v FROM numbers(100));
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT '1'::int256 AS v FROM numbers(100));
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT '1'::uint128 AS v FROM numbers(100));
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT '1'::uint256 AS v FROM numbers(100));

-- Decimal types
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT '1.001'::decimal(3, 3) AS v FROM numbers(100));

-- Other types
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT 'a'::string AS v); -- { serverError 43 }
SELECT to_type_name(sum_count(v)), sum_count(v) FROM (SELECT now()::dateTime AS v); -- { serverError 43 }


-- SumCountIf
SELECT sum_count_if(n, n > 10) FROM (SELECT number AS n FROM system.numbers LIMIT 100 );
SELECT sum_count_if(n, n > 10) FROM (SELECT to_nullable(number) AS n FROM system.numbers LIMIT 100);
SELECT sum_count_if(n, n > 10) FROM (SELECT if(number % 2 == 0, number, NULL) AS n FROM system.numbers LIMIT 100);
