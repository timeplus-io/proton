SELECT bitCount(number) FROM numbers(10);
SELECT avg(bitCount(number)) FROM numbers(256);

SELECT bitCount(0);
SELECT bitCount(1);
SELECT bitCount(-1);

SELECT bitCount(to_int64(-1));
SELECT bitCount(to_int32(-1));
SELECT bitCount(to_int16(-1));
SELECT bitCount(to_int8(-1));

SELECT x, bitCount(x), hex(reinterpret_as_string(x)) FROM VALUES ('x float64', (1), (-1), (inf));
