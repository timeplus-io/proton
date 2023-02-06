SELECT 0.1::decimal(38, 38) AS c;
EXPLAIN SYNTAX SELECT 0.1::decimal(38, 38) AS c;

SELECT [1, 2, 3]::array(uint32) AS c;
EXPLAIN SYNTAX SELECT [1, 2, 3]::array(uint32) AS c;

SELECT 'abc'::fixed_string(3) AS c;
EXPLAIN SYNTAX SELECT 'abc'::fixed_string(3) AS c;

SELECT 123::string AS c;
EXPLAIN SYNTAX SELECT 123::string AS c;

SELECT 1::int8 AS c;
EXPLAIN SYNTAX SELECT 1::int8 AS c;

SELECT [1, 1 + 1, 1 + 2]::array(uint32) AS c;
EXPLAIN SYNTAX SELECT [1, 1 + 1, 1 + 2]::array(uint32) AS c;

SELECT '2010-10-10'::Date AS c;
EXPLAIN SYNTAX SELECT '2010-10-10'::Date AS c;

SELECT '2010-10-10'::DateTime('UTC') AS c;
EXPLAIN SYNTAX SELECT '2010-10-10'::DateTime('UTC') AS c;

SELECT ['2010-10-10', '2010-10-10']::array(Date) AS c;
EXPLAIN SYNTAX SELECT ['2010-10-10', '2010-10-10']::array(Date);

SELECT (1 + 2)::uint32 AS c;
EXPLAIN SYNTAX SELECT (1 + 2)::uint32 AS c;

SELECT (0.1::decimal(4, 4) * 5)::float64 AS c;
EXPLAIN SYNTAX SELECT (0.1::decimal(4, 4) * 5)::float64 AS c;

SELECT number::uint8 AS c, to_type_name(c) FROM numbers(1);
EXPLAIN SYNTAX SELECT number::uint8 AS c, to_type_name(c) FROM numbers(1);

SELECT (0 + 1 + 2 + 3 + 4)::Date AS c;
EXPLAIN SYNTAX SELECT (0 + 1 + 2 + 3 + 4)::Date AS c;

SELECT (0.1::decimal(4, 4) + 0.2::decimal(4, 4) + 0.3::decimal(4, 4))::decimal(4, 4) AS c;
EXPLAIN SYNTAX SELECT (0.1::decimal(4, 4) + 0.2::decimal(4, 4) + 0.3::decimal(4, 4))::decimal(4, 4) AS c;

SELECT [[1][1]]::array(uint32);
SELECT [[1, 2, 3], [], [1]]::array(array(uint32));
SELECT [[], []]::array(array(uint32));
