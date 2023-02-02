SELECT to_int32(-199) % 200 as k, to_type_name(k);
SELECT to_int32(-199) % to_uint16(200) as k, to_type_name(k);
SELECT to_int32(-199) % to_uint32(200) as k, to_type_name(k);
SELECT to_int32(-199) % to_uint64(200) as k, to_type_name(k);

SELECT to_int32(-199) % to_int16(-200) as k, to_type_name(k);

SELECT 199 % -10 as k, to_type_name(k);
SELECT 199 % -200 as k, to_type_name(k);

SELECT to_float64(-199) % 200 as k, to_type_name(k);
SELECT -199 % to_float64(200) as k, to_type_name(k);
