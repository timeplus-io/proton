SELECT '-- generate_uuidv7 --';
SELECT to_type_name(generate_uuidv7());
SELECT substring(hex(generate_uuidv7()), 13, 1); -- check version bits
SELECT bit_and(bit_shift_right(to_uint128(generate_uuidv7()), 62), 3); -- check variant bits
SELECT generate_uuidv7(1) = generate_uuidv7(2);
SELECT generate_uuidv7() = generate_uuidv7(1);
SELECT generate_uuidv7(1) = generate_uuidv7(1);

SELECT '-- generate_uuidv7_thread_monotonic --';
SELECT to_type_name(generate_uuidv7_thread_monotonic());
SELECT substring(hex(generate_uuidv7_thread_monotonic()), 13, 1); -- check version bits
SELECT bit_and(bit_shift_right(to_uint128(generate_uuidv7_thread_monotonic()), 62), 3); -- check variant bits
SELECT generate_uuidv7_thread_monotonic(1) = generate_uuidv7_thread_monotonic(2);
SELECT generate_uuidv7_thread_monotonic() = generate_uuidv7_thread_monotonic(1);
SELECT generate_uuidv7_thread_monotonic(1) = generate_uuidv7_thread_monotonic(1);

SELECT '-- generate_uuidv7_non_monotonic --';
SELECT to_type_name(generate_uuidv7_non_monotonic());
SELECT substring(hex(generate_uuidv7_non_monotonic()), 13, 1); -- check version bits
SELECT bit_and(bit_shift_right(to_uint128(generate_uuidv7_non_monotonic()), 62), 3); -- check variant bits
SELECT generate_uuidv7_non_monotonic(1) = generate_uuidv7_non_monotonic(2);
SELECT generate_uuidv7_non_monotonic() = generate_uuidv7_non_monotonic(1);
SELECT generate_uuidv7_non_monotonic(1) = generate_uuidv7_non_monotonic(1);
