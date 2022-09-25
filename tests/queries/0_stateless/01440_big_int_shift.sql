SELECT bit_shift_left(to_int128(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(127) ORDER BY number;
SELECT bit_shift_left(toInt256(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(255) ORDER BY number;
SELECT bit_shift_left(toUInt256(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(256) ORDER BY number;
