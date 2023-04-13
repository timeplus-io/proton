-- Tags: long

 

SELECT 'value vs value';

SELECT to_int8(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int8(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_int8(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int8(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int8(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_int16(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int16(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_int16(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int16(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int16(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_int32(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int32(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_int32(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int32(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int32(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_int64(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int64(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int64(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int64(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_int64(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_int64(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_int64(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_uint8(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_uint8(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint8(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint8(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_uint16(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_uint16(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint16(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint16(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_uint32(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_uint32(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint32(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint32(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_uint64(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_uint64(0) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_uint64(0) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_uint64(0) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT to_date(0) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_date('2000-01-01') AS x, to_datetime('2000-01-01 00:00:01', 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_date(0) AS x, to_decimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_decimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT to_date(0) AS x, to_decimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }

SELECT to_datetime(0, 'Europe/Moscow') AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime('2000-01-01 00:00:00', 'Europe/Moscow') AS x, to_date('2000-01-02') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_decimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_decimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT to_datetime(0, 'Europe/Moscow') AS x, to_decimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }

SELECT 'column vs value';

SELECT materialize(to_int8(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int8(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_int8(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int8(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int8(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_int16(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int16(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_int16(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int16(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int16(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_int32(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int32(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_int32(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int32(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int32(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_int64(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int64(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int64(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int64(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_int64(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_int64(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_int64(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_uint8(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_uint8(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint8(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint8(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_uint16(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_uint16(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint16(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint16(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_uint32(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_uint32(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint32(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint32(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_uint64(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_uint64(0)) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_uint64(0)) AS x, to_decimal32(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_decimal64(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_uint64(0)) AS x, to_decimal128(1, 0) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);

SELECT materialize(to_date(0)) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_date(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_date('2000-01-01')) AS x, to_datetime('2000-01-01 00:00:01', 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_date(0)) AS x, to_decimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_decimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }
SELECT materialize(to_date(0)) AS x, to_decimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 43 }

SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_int8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_int16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_int32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_int64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_uint8(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_uint16(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_uint32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_uint64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_float32(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_float64(1) AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Europe/Moscow')) AS x, to_date('2000-01-02') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_datetime(1, 'Europe/Moscow') AS y, ((x > y) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z);
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_decimal32(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_decimal64(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
SELECT materialize(to_datetime(0, 'Europe/Moscow')) AS x, to_decimal128(1, 0) AS y, ((x = 0) ? x : y) AS z, to_type_name(x), to_type_name(y), to_type_name(z); -- { serverError 386 }
