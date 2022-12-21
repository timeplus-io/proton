-- Tags: no-fasttest

SELECT JSONExtract('{"string_value":null}', 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT JSONExtract('{"string_value":null}', 'string_value', 'string') as x, to_type_name(x);
SELECT JSONExtract(to_nullable('{"string_value":null}'), 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT JSONExtract(to_nullable('{"string_value":null}'), 'string_value', 'string') as x, to_type_name(x);
SELECT JSONExtract(NULL, 'string_value', 'nullable(string)') as x, to_type_name(x);
SELECT JSONExtract(NULL, 'string_value', 'string') as x, to_type_name(x);
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT array_join([2, NULL, 2147483646, 65535, 65535, 3]) AS idx);

SELECT JSONExtractInt('[1]', to_nullable(1));
SELECT JSONExtractBool('[1]', to_nullable(1));
SELECT JSONExtractFloat('[1]', to_nullable(1));
SELECT JSONExtractString('["a"]', to_nullable(1));

SELECT JSONExtractArrayRaw('["1"]', to_nullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValuesRaw('["1"]', to_nullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValues('["1"]', to_nullable(1)); -- { serverError ILLEGAL_COLUMN }

SELECT JSONExtract('[1]', to_nullable(1), 'nullable(int)');
SELECT JSONExtract('[1]', to_nullable(1), 'nullable(uint8)');
SELECT JSONExtract('[1]', to_nullable(1), 'nullable(Bool)');
SELECT JSONExtract('[1]', to_nullable(1), 'nullable(Float)');
SELECT JSONExtract('["a"]', to_nullable(1), 'nullable(string)');
SELECT JSONExtract('["a"]', to_nullable(1), 'nullable(int)');
SELECT JSONExtract('["-a"]', to_nullable(1), 'nullable(int)');
