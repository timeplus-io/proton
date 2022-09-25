SELECT reinterpretAsFloat32(CAST(123456 AS uint32));
SELECT reinterpretAsUInt32(CAST(1.23456 AS float32));
SELECT reinterpretAsFloat32(CAST(123456 AS int32));
SELECT reinterpretAsInt32(CAST(1.23456 AS float32));
SELECT reinterpret_as_float64(CAST(123456 AS uint64));
SELECT reinterpretAsUInt64(CAST(1.23456 AS float64));
SELECT reinterpret_as_float64(CAST(123456 AS int64));
SELECT reinterpretAsInt64(CAST(1.23456 AS float64));
