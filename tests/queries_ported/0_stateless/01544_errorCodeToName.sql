SELECT error_code_to_name(to_uint32(-1));
SELECT error_code_to_name(-1);
SELECT error_code_to_name(950); /* gap in error codes */
SELECT error_code_to_name(0);
SELECT error_code_to_name(1);
