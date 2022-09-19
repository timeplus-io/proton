SELECT (0.1, 0.2)::tuple(Decimal(75, 70), Decimal(75, 70));
EXPLAIN SYNTAX SELECT (0.1, 0.2)::tuple(Decimal(75, 70), Decimal(75, 70));

SELECT 0.1 :: Decimal(4, 4);
EXPLAIN SYNTAX SELECT 0.1 :: Decimal(4, 4);

SELECT [1, 2, 3] :: array(int32);
EXPLAIN SYNTAX SELECT [1, 2, 3] :: array(int32);

SELECT [1::uint32, 2::uint32]::array(uint64);
EXPLAIN SYNTAX SELECT [1::uint32, 2::uint32]::array(uint64);

SELECT [[1, 2]::array(uint32), [3]]::array(array(uint64));
EXPLAIN SYNTAX SELECT [[1, 2]::array(uint32), [3]]::array(array(uint64));

SELECT [[1::UInt16, 2::UInt16]::array(uint32), [3]]::array(array(uint64));
EXPLAIN SYNTAX SELECT [[1::UInt16, 2::UInt16]::array(uint32), [3]]::array(array(uint64));

SELECT [(1, 'a'), (3, 'b')]::nested(u uint8, s string) AS t, to_type_name(t);
