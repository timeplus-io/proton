-- Tags: no-fasttest

SELECT json_extract('{"string_value":null}', 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT json_extract('{"string_value":null}', 'string_value', 'string') as x, to_type_name(x);
SELECT json_extract(to_nullable('{"string_value":null}'), 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT json_extract(to_nullable('{"string_value":null}'), 'string_value', 'string') as x, to_type_name(x);
SELECT json_extract(NULL, 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT json_extract(NULL, 'string_value', 'string') as x, to_type_name(x);
SELECT json_extract_string('["a", "b", "c", "d", "e"]', idx) FROM (SELECT array_join([2, NULL, 2147483646, 65535, 65535, 3]) AS idx);

SELECT json_extract_int('[1]', to_nullable(1));
SELECT json_extract_bool('[1]', to_nullable(1));
SELECT json_extract_float('[1]', to_nullable(1));
SELECT json_extract_string('["a"]', to_nullable(1));

SELECT json_extract_array_raw('["1"]', to_nullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT json_extract_keys_and_values_raw('["1"]', to_nullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT json_extract_keys_and_values('["1"]', to_nullable(1)); -- { serverError ILLEGAL_COLUMN }

SELECT json_extract('[1]', to_nullable(1), 'nullable(int)');
SELECT json_extract('[1]', to_nullable(1), 'nullable(uint8)');
SELECT json_extract('[1]', to_nullable(1), 'nullable(bool)');
SELECT json_extract('[1]', to_nullable(1), 'nullable(float)');
SELECT json_extract('["a"]', to_nullable(1), 'nullable(string)');
SELECT json_extract('["a"]', to_nullable(1), 'nullable(int)');
SELECT json_extract('["-a"]', to_nullable(1), 'nullable(int)');
