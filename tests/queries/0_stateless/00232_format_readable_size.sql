WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : to_uint64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : to_int32(x) AS z
SELECT formatReadableSize(x), formatReadableSize(y), formatReadableSize(z)
FROM system.numbers
LIMIT 70;
