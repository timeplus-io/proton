SELECT  least(to_int8(127), to_int128(0)) as x, least(to_int8(127), to_int128(128)) as x2,
        least(to_int8(-128), to_int128(0)) as x3, least(to_int8(-128), to_int128(-129)) as x4,
        greatest(to_int8(127), to_int128(0)) as y, greatest(to_int8(127), to_int128(128)) as y2,
        greatest(to_int8(-128), to_int128(0)) as y3, greatest(to_int8(-128), to_int128(-129)) as y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int8(127), to_int256(0)) as x, least(to_int8(127), to_int256(128)) as x2,
        least(to_int8(-128), to_int256(0)) as x3, least(to_int8(-128), to_int256(-129)) as x4,
        greatest(to_int8(127), to_int256(0)) as y, greatest(to_int8(127), to_int256(128)) as y2,
        greatest(to_int8(-128), to_int256(0)) as y3, greatest(to_int8(-128), to_int256(-129)) as y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int64(9223372036854775807), to_int128(0)) as x, least(to_int64(9223372036854775807), to_int128('9223372036854775808')) as x2,
        least(to_int64(-9223372036854775808), to_int128(0)) as x3, least(to_int64(-9223372036854775808), to_int128('-9223372036854775809')) as x4,
        greatest(to_int64(9223372036854775807), to_int128(0)) as y, greatest(to_int64(9223372036854775807), to_int128('9223372036854775808')) as y2,
        greatest(to_int64(-9223372036854775808), to_int128(0)) as y3, greatest(to_int64(-9223372036854775808), to_int128('-9223372036854775809')) as y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_int64(9223372036854775807), to_int256(0)) as x, least(to_int64(9223372036854775807), to_int256('9223372036854775808')) as x2,
        least(to_int64(-9223372036854775808), to_int256(0)) as x3, least(to_int64(-9223372036854775808), to_int256('-9223372036854775809')) as x4,
        greatest(to_int64(9223372036854775807), to_int256(0)) as y, greatest(to_int64(9223372036854775807), to_int256('9223372036854775808')) as y2,
        greatest(to_int64(-9223372036854775808), to_int256(0)) as y3, greatest(to_int64(-9223372036854775808), to_int256('-9223372036854775809')) as y4,
        to_type_name(x), to_type_name(y);

SELECT  least(to_uint8(255), to_uint256(0)) as x, least(to_uint8(255), to_uint256(256)) as x2,
        greatest(to_uint8(255), to_uint256(0)) as y, greatest(to_uint8(255), to_uint256(256)) as y2,
        to_type_name(x), to_type_name(y);

SELECT  least(to_uint64('18446744073709551615'), to_uint256(0)) as x, least(to_uint64('18446744073709551615'), to_uint256('18446744073709551616')) as x2,
        greatest(to_uint64('18446744073709551615'), to_uint256(0)) as y, greatest(to_uint64('18446744073709551615'), to_uint256('18446744073709551616')) as y2,
        to_type_name(x), to_type_name(y);

SELECT least(to_uint32(0), to_int256(0)), greatest(to_int32(0), to_uint256(0)); -- { serverError 43 }
SELECT least(to_int32(0), to_uint256(0)), greatest(to_int32(0), to_uint256(0)); -- { serverError 43 }
