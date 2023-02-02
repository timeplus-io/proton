SELECT reinterpret_as_float32(CAST(123456 AS uint32));
SELECT reinterpret_as_uint32(CAST(1.23456 AS float32));
SELECT reinterpret_as_float32(CAST(123456 AS int32));
SELECT reinterpret_as_int32(CAST(1.23456 AS float32));
SELECT reinterpret_as_float64(CAST(123456 AS uint64));
SELECT reinterpret_as_uint64(CAST(1.23456 AS float64));
SELECT reinterpret_as_float64(CAST(123456 AS int64));
SELECT reinterpret_as_int64(CAST(1.23456 AS float64));
