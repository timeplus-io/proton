SELECT DISTINCT bit_xor(materialize(to_fixed_string('abc', 3)), to_fixed_string('\x00\x01\x02', 3)) FROM numbers(10);
SELECT DISTINCT bit_xor(materialize(to_fixed_string('abcdef', 6)), to_fixed_string('\x00\x01\x02\x03\x04\x05', 6)) FROM numbers(10);

SELECT DISTINCT bit_xor(to_fixed_string('\x00\x01\x02', 3), materialize(to_fixed_string('abc', 3))) FROM numbers(10);
SELECT DISTINCT bit_xor(to_fixed_string('\x00\x01\x02\x03\x04\x05', 6), materialize(to_fixed_string('abcdef', 6))) FROM numbers(10);
