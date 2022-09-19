SELECT bitShiftLeft(to_int128(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(127) ORDER BY number;
SELECT bitShiftLeft(toInt256(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(255) ORDER BY number;
SELECT bitShiftLeft(toUInt256(1), number) x, bitShiftRight(x, number) y, to_type_name(x), to_type_name(y) FROM numbers(256) ORDER BY number;
