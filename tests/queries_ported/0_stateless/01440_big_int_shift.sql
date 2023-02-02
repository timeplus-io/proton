SELECT bit_shift_left(to_int128(1), number) as x, bit_shift_right(x, number) as y, to_type_name(x), to_type_name(y) FROM numbers(127) ORDER BY number;
SELECT bit_shift_left(to_int256(1), number) as x, bit_shift_right(x, number) as y, to_type_name(x), to_type_name(y) FROM numbers(255) ORDER BY number;
SELECT bit_shift_left(to_uint256(1), number) as x, bit_shift_right(x, number) as y, to_type_name(x), to_type_name(y) FROM numbers(256) ORDER BY number;
