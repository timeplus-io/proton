SELECT number, bit_test_any(to_uint8(1 + 4 + 16 + 64), number) FROM numbers(100);
SELECT number, bit_test_all(to_uint8(1 + 4 + 16 + 64), number) FROM numbers(100);
