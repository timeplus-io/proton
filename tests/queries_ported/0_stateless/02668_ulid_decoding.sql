-- Tags: no-fasttest

SELECT dateDiff('minute', ulid_string_to_datetime(generate_ulid()), now()) <= 1;
SELECT to_timezone(ulid_string_to_datetime('01GWJWKW30MFPQJRYEAF4XFZ9E'), 'America/Costa_Rica');
SELECT ulid_string_to_datetime('01GWJWKW30MFPQJRYEAF4XFZ9E', 'America/Costa_Rica');
SELECT ulid_string_to_datetime('01GWJWKW30MFPQJRYEAF4XFZ9', 'America/Costa_Rica'); -- { serverError ILLEGAL_COLUMN }
SELECT ulid_string_to_datetime('01GWJWKW30MFPQJRYEAF4XFZ9E', 'America/Costa_Ric'); -- { serverError BAD_ARGUMENTS }
SELECT ulid_string_to_datetime('01GWJWKW30MFPQJRYEAF4XFZ9E0'); -- { serverError ILLEGAL_COLUMN }
SELECT ulid_string_to_datetime(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ulid_string_to_datetime(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
