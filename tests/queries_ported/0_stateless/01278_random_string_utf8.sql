SELECT random_string_utf8('string'); -- { serverError 43 }
SELECT length_utf8(random_string_utf8(100));
SELECT to_type_name(random_string_utf8(10));
SELECT is_valid_utf8(random_string_utf8(100000));
SELECT random_string_utf8(0);
