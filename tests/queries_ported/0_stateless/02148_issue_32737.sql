SELECT fuzz_bits(to_fixed_string('', 200), 0.99) from numbers(1) FORMAT Null;
SELECT fuzz_bits(to_fixed_string('', 200), 0.99) from numbers(128) FORMAT Null;
SELECT fuzz_bits(to_fixed_string('', 200), 0.99) from numbers(60000) FORMAT Null;
