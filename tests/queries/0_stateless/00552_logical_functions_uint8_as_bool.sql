
-- Test that uint8 type is processed correctly as bool

SELECT
    1 AND 2,
    2 AND 4,
    1 AND 2 AND 4,
    1 OR 2,
    2 OR 4,
    1 OR 2 OR 4
;

SELECT
    to_uint8(bitAnd(number, 4)) AS a,
    to_uint8(bitAnd(number, 2)) AS b,
    to_uint8(bitAnd(number, 1)) AS c,
    a AND b AND c AS AND,
    a OR b OR c AS OR
FROM numbers(8)
;
