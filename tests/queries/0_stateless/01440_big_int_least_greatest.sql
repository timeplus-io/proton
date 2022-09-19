SELECT  least(to_int8(127), to_int128(0)) x, least(to_int8(127), to_int128(128)) x2,
        least(to_int8(-128), to_int128(0)) x3, least(to_int8(-128), to_int128(-129)) x4,
        greatest(to_int8(127), to_int128(0)) y, greatest(to_int8(127), to_int128(128)) y2,
        greatest(to_int8(-128), to_int128(0)) y3, greatest(to_int8(-128), to_int128(-129)) y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int8(127), toInt256(0)) x, least(to_int8(127), toInt256(128)) x2,
        least(to_int8(-128), toInt256(0)) x3, least(to_int8(-128), toInt256(-129)) x4,
        greatest(to_int8(127), toInt256(0)) y, greatest(to_int8(127), toInt256(128)) y2,
        greatest(to_int8(-128), toInt256(0)) y3, greatest(to_int8(-128), toInt256(-129)) y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int64(9223372036854775807), to_int128(0)) x, least(to_int64(9223372036854775807), to_int128('9223372036854775808')) x2,
        least(to_int64(-9223372036854775808), to_int128(0)) x3, least(to_int64(-9223372036854775808), to_int128('-9223372036854775809')) x4,
        greatest(to_int64(9223372036854775807), to_int128(0)) y, greatest(to_int64(9223372036854775807), to_int128('9223372036854775808')) y2,
        greatest(to_int64(-9223372036854775808), to_int128(0)) y3, greatest(to_int64(-9223372036854775808), to_int128('-9223372036854775809')) y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int64(9223372036854775807), toInt256(0)) x, least(to_int64(9223372036854775807), toInt256('9223372036854775808')) x2,
        least(to_int64(-9223372036854775808), toInt256(0)) x3, least(to_int64(-9223372036854775808), toInt256('-9223372036854775809')) x4,
        greatest(to_int64(9223372036854775807), toInt256(0)) y, greatest(to_int64(9223372036854775807), toInt256('9223372036854775808')) y2,
        greatest(to_int64(-9223372036854775808), toInt256(0)) y3, greatest(to_int64(-9223372036854775808), toInt256('-9223372036854775809')) y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_uint8(255), toUInt256(0)) x, least(to_uint8(255), toUInt256(256)) x2,
        greatest(to_uint8(255), toUInt256(0)) y, greatest(to_uint8(255), toUInt256(256)) y2,
        to_type_name(x), to_type_name(y);

SELECT  least(to_uint64('18446744073709551615'), toUInt256(0)) x, least(to_uint64('18446744073709551615'), toUInt256('18446744073709551616')) x2,
        greatest(to_uint64('18446744073709551615'), toUInt256(0)) y, greatest(to_uint64('18446744073709551615'), toUInt256('18446744073709551616')) y2,
        to_type_name(x), to_type_name(y);

SELECT least(to_uint32(0), toInt256(0)), greatest(to_int32(0), toUInt256(0)); -- { serverError 43 }
SELECT least(to_int32(0), toUInt256(0)), greatest(to_int32(0), toUInt256(0)); -- { serverError 43 }
