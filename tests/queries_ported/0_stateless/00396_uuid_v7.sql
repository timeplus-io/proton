SELECT '-- uuid_to_num --';
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), 1) = uuid_string_to_num('00112233-4455-6677-8899-aabbccddeeff', 1);
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), 2) = uuid_string_to_num('00112233-4455-6677-8899-aabbccddeeff', 2);
SELECT uuid_to_num();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), 1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), 3); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT uuid_to_num('00112233-4455-6677-8899-aabbccddeeff', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), '1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uuid_to_num(to_uuid('00112233-4455-6677-8899-aabbccddeeff'), materialize(1)); -- { serverError ILLEGAL_COLUMN }

SELECT '-- uuidv7_to_datetime --';
SELECT uuidv7_to_datetime(to_uuid('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York');
SELECT uuidv7_to_datetime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT uuidv7_to_datetime(to_uuid('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 1); -- { serverError ILLEGAL_COLUMN }
SELECT uuidv7_to_datetime(to_uuid('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York', 1);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT uuidv7_to_datetime('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uuidv7_to_datetime(to_uuid('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/NewYork'); -- { serverError BAD_ARGUMENTS }
SELECT uuidv7_to_datetime(to_uuid('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), materialize('America/New_York')); -- { serverError ILLEGAL_COLUMN }

