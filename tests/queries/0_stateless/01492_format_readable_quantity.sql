WITH round(exp(number), 6) AS x, to_uint64(x) AS y, to_int32(x) AS z
SELECT formatReadableQuantity(x), formatReadableQuantity(y), formatReadableQuantity(z)
FROM system.numbers
LIMIT 45;
