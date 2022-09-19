SELECT to_uint8(assumeNotNull(cast(cast(NULL, 'Nullable(string)'), 'Nullable(Enum8(\'Hello\' = 1))')));
