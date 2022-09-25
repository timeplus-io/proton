-- Integer types are added as integers
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::uint64 AS v UNION ALL SELECT '1'::uint64 AS v UNION ALL SELECT '1'::uint64 AS v);
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::Nullable(uint64) AS v UNION ALL SELECT '1'::Nullable(uint64) AS v UNION ALL SELECT '1'::Nullable(uint64) AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::low_cardinality(uint64) AS v UNION ALL SELECT '1'::low_cardinality(uint64) AS v UNION ALL SELECT '1'::low_cardinality(uint64) AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::low_cardinality(Nullable(uint64)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(uint64)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(uint64)) AS v );

-- float64 types are added as float64
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::float64 AS v UNION ALL SELECT '1'::float64 AS v UNION ALL SELECT '1'::float64 AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::Nullable(float64) AS v UNION ALL SELECT '1'::Nullable(float64) AS v UNION ALL SELECT '1'::Nullable(float64) AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::low_cardinality(float64) AS v UNION ALL SELECT '1'::low_cardinality(float64) AS v UNION ALL SELECT '1'::low_cardinality(float64) AS v);
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '9007199254740992'::low_cardinality(Nullable(float64)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(float64)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(float64)) AS v );

-- Float32 are added as float64
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '16777216'::Float32 AS v UNION ALL SELECT '1'::Float32 AS v UNION ALL SELECT '1'::Float32 AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '16777216'::Nullable(Float32) AS v UNION ALL SELECT '1'::Nullable(Float32) AS v UNION ALL SELECT '1'::Nullable(Float32) AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '16777216'::low_cardinality(Float32) AS v UNION ALL SELECT '1'::low_cardinality(Float32) AS v UNION ALL SELECT '1'::low_cardinality(Float32) AS v );
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '16777216'::low_cardinality(Nullable(Float32)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(Float32)) AS v UNION ALL SELECT '1'::low_cardinality(Nullable(Float32)) AS v );

-- Small integer types use their sign/unsigned 64 byte supertype
SELECT to_type_name(sumCount(number::int8)), sumCount(number::int8) FROM numbers(120);
SELECT to_type_name(sumCount(number::uint8)), sumCount(number::uint8) FROM numbers(250);

-- Greater integers use their own type
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '1'::Int128 AS v FROM numbers(100));
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '1'::Int256 AS v FROM numbers(100));
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '1'::UInt128 AS v FROM numbers(100));
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '1'::UInt256 AS v FROM numbers(100));

-- Decimal types
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT '1.001'::Decimal(3, 3) AS v FROM numbers(100));

-- Other types
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT 'a'::string AS v); -- { serverError 43 }
SELECT to_type_name(sumCount(v)), sumCount(v) FROM (SELECT now()::DateTime AS v); -- { serverError 43 }


-- Sumcount_if
SELECT sumcount_if(n, n > 10) FROM (SELECT number AS n FROM system.numbers LIMIT 100 );
SELECT sumcount_if(n, n > 10) FROM (SELECT toNullable(number) AS n FROM system.numbers LIMIT 100);
SELECT sumcount_if(n, n > 10) FROM (SELECT If(number % 2 == 0, number, NULL) AS n FROM system.numbers LIMIT 100);
