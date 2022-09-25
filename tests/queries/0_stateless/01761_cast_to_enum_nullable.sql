SELECT to_uint8(assumeNotNull(cast(cast(NULL, 'nullable(string)'), 'nullable(enum8(\'Hello\' = 1))')));
