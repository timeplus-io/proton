SELECT nan = to_uint8(0), nan != to_uint8(0), nan < to_uint8(0), nan > to_uint8(0), nan <= to_uint8(0), nan >= to_uint8(0);
SELECT nan = to_int8(0), nan != to_int8(0), nan < to_int8(0), nan > to_int8(0), nan <= to_int8(0), nan >= to_int8(0);
SELECT nan = to_uint16(0), nan != to_uint16(0), nan < to_uint16(0), nan > to_uint16(0), nan <= to_uint16(0), nan >= to_uint16(0);
SELECT nan = to_int16(0), nan != to_int16(0), nan < to_int16(0), nan > to_int16(0), nan <= to_int16(0), nan >= to_int16(0);
SELECT nan = to_uint32(0), nan != to_uint32(0), nan < to_uint32(0), nan > to_uint32(0), nan <= to_uint32(0), nan >= to_uint32(0);
SELECT nan = to_int32(0), nan != to_int32(0), nan < to_int32(0), nan > to_int32(0), nan <= to_int32(0), nan >= to_int32(0);
SELECT nan = to_uint64(0), nan != to_uint64(0), nan < to_uint64(0), nan > to_uint64(0), nan <= to_uint64(0), nan >= to_uint64(0);
SELECT nan = to_int64(0), nan != to_int64(0), nan < to_int64(0), nan > to_int64(0), nan <= to_int64(0), nan >= to_int64(0);
SELECT nan = to_float32(0.0), nan != to_float32(0.0), nan < to_float32(0.0), nan > to_float32(0.0), nan <= to_float32(0.0), nan >= to_float32(0.0);
SELECT nan = to_float64(0.0), nan != to_float64(0.0), nan < to_float64(0.0), nan > to_float64(0.0), nan <= to_float64(0.0), nan >= to_float64(0.0);

SELECT -nan = to_uint8(0), -nan != to_uint8(0), -nan < to_uint8(0), -nan > to_uint8(0), -nan <= to_uint8(0), -nan >= to_uint8(0);
SELECT -nan = to_int8(0), -nan != to_int8(0), -nan < to_int8(0), -nan > to_int8(0), -nan <= to_int8(0), -nan >= to_int8(0);
SELECT -nan = to_uint16(0), -nan != to_uint16(0), -nan < to_uint16(0), -nan > to_uint16(0), -nan <= to_uint16(0), -nan >= to_uint16(0);
SELECT -nan = to_int16(0), -nan != to_int16(0), -nan < to_int16(0), -nan > to_int16(0), -nan <= to_int16(0), -nan >= to_int16(0);
SELECT -nan = to_uint32(0), -nan != to_uint32(0), -nan < to_uint32(0), -nan > to_uint32(0), -nan <= to_uint32(0), -nan >= to_uint32(0);
SELECT -nan = to_int32(0), -nan != to_int32(0), -nan < to_int32(0), -nan > to_int32(0), -nan <= to_int32(0), -nan >= to_int32(0);
SELECT -nan = to_uint64(0), -nan != to_uint64(0), -nan < to_uint64(0), -nan > to_uint64(0), -nan <= to_uint64(0), -nan >= to_uint64(0);
SELECT -nan = to_int64(0), -nan != to_int64(0), -nan < to_int64(0), -nan > to_int64(0), -nan <= to_int64(0), -nan >= to_int64(0);
SELECT -nan = to_float32(0.0), -nan != to_float32(0.0), -nan < to_float32(0.0), -nan > to_float32(0.0), -nan <= to_float32(0.0), -nan >= to_float32(0.0);
SELECT -nan = to_float64(0.0), -nan != to_float64(0.0), -nan < to_float64(0.0), -nan > to_float64(0.0), -nan <= to_float64(0.0), -nan >= to_float64(0.0);

--SELECT 1 % nan, nan % 1, pow(x, 1), pow(1, x); -- TODO
SELECT 1 + nan, 1 - nan, nan - 1, 1 * nan, 1 / nan, nan / 1;
SELECT nan AS x, is_finite(exp(x)) /* exp(nan) is allowed to return inf */, exp2(x), exp10(x), log(x), log2(x), log10(x), sqrt(x), cbrt(x);
SELECT nan AS x, erf(x), erfc(x), lgamma(x), tgamma(x);
SELECT nan AS x, sin(x), cos(x), tan(x), asin(x), acos(x), atan(x);

SELECT min(x), max(x) FROM (SELECT array_join([to_float32(0.0), nan, to_float32(1.0), to_float32(-1.0)]) AS x);
SELECT min(x), max(x) FROM (SELECT array_join([to_float64(0.0), -nan, to_float64(1.0), to_float64(-1.0)]) AS x);
