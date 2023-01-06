SELECT (to_decimal128(materialize('1'), 0), to_decimal128('2', 0)) < (to_decimal128('1', 0), to_decimal128('2', 0));
