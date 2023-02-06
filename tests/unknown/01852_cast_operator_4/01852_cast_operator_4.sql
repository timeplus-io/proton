SELECT [3,4,5][1]::int32;
EXPLAIN SYNTAX SELECT [3,4,5][1]::int32;

SELECT [3,4,5]::array(int64)[2]::int8;
EXPLAIN SYNTAX SELECT [3,4,5]::array(int64)[2]::int8;

SELECT [1,2,3]::array(uint64)[[number, number]::array(uint8)[number]::uint64]::uint8 from numbers(3);
EXPLAIN SYNTAX SELECT [1,2,3]::array(uint64)[[number, number]::array(uint8)[number]::uint64]::uint8 from numbers(3);

WITH [3,4,5] AS x SELECT x[1]::int32;
EXPLAIN SYNTAX WITH [3,4,5] AS x SELECT x[1]::int32;

SELECT tuple(3,4,5).1::int32;
EXPLAIN SYNTAX SELECT tuple(3,4,5).1::int32;

SELECT tuple(3,4,5)::Tuple(uint64, uint64, uint64).2::int32;
EXPLAIN SYNTAX SELECT tuple(3,4,5)::Tuple(uint64, uint64, uint64).1::int32;

WITH tuple(3,4,5) AS x SELECT x.1::int32;
EXPLAIN SYNTAX WITH tuple(3,4,5) AS x SELECT x.1::int32;
