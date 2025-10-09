select '---------- generate_uuidv4() Tests ----------';
SELECT to_type_name(uuid()) = to_type_name(generate_uuidv4());
SELECT substring(hex(uuid()), 13, 1) = substring(hex(generate_uuidv4()), 13, 1);
SELECT bit_and(bit_shift_right(to_uint128(uuid()), 62), 3) = 
       bit_and(bit_shift_right(to_uint128(generate_uuidv4()), 62), 3);
SELECT uuid(1) = generate_uuidv4(1);
SELECT uuid(1) = generate_uuidv4(2);


select '---------- uuid7() Tests ----------';
SELECT to_type_name(generate_uuidv7()) = to_type_name(uuid7());
SELECT substring(hex(generate_uuidv7()), 13, 1) = substring(hex(uuid7()), 13, 1);
SELECT bit_and(bit_shift_right(to_uint128(generate_uuidv7()), 62), 3) = 
       bit_and(bit_shift_right(to_uint128(uuid7()), 62), 3);
SELECT generate_uuidv7(1) = uuid7(1);
SELECT generate_uuidv7(1) = uuid7(2);


