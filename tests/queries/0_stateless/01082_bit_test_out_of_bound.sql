SELECT number, bitTestAny(to_uint8(1 + 4 + 16 + 64), number) FROM numbers(100);
SELECT number, bitTestAll(to_uint8(1 + 4 + 16 + 64), number) FROM numbers(100);
