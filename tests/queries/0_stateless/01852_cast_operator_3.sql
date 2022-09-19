SELECT -1::int32;
EXPLAIN SYNTAX SELECT -1::int32;

SELECT -0.1::Decimal(38, 38);
EXPLAIN SYNTAX SELECT -0.1::Decimal(38, 38);

SELECT -0.111::float64;
EXPLAIN SYNTAX SELECT -0.111::float64;

SELECT [-1, 2, -3]::array(int32);
EXPLAIN SYNTAX SELECT [-1, 2, -3]::array(int32);

SELECT [-1.1, 2, -3]::array(float64);
EXPLAIN SYNTAX SELECT [-1.1, 2, -3]::array(float64);
