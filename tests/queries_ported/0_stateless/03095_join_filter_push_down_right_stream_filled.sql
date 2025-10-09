DROP STREAM IF EXISTS t1__fuzz_0;
CREATE STREAM t1__fuzz_0
(
    `x` uint8,
    `str` string
)
    ENGINE = MergeTree ORDER BY x;

INSERT INTO t1__fuzz_0 SELECT number, to_string(number) FROM numbers(10);

DROP STREAM IF EXISTS left_join__fuzz_2;
CREATE STREAM left_join__fuzz_2
(
    `x` uint32,
    `s` low_cardinality(string)
) ENGINE = Join(`ALL`, LEFT, x);

INSERT INTO left_join__fuzz_2 SELECT number, to_string(number) FROM numbers(10);

SELECT 14 FROM t1__fuzz_0 LEFT JOIN left_join__fuzz_2 USING (x)
WHERE point_in_polygon(materialize((-inf, 1023)), [(5, 0.9998999834060669), (1.1920928955078125e-7, 100.0000991821289), (1.000100016593933, 100.0000991821289)])
ORDER BY to_nullable('202.79.32.10') DESC NULLS LAST, to_nullable(to_low_cardinality(to_uint256(14))) ASC, x DESC NULLS LAST;

DROP STREAM t1__fuzz_0;
DROP STREAM left_join__fuzz_2;
