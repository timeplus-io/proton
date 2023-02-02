SELECT to_uint8(assume_not_null(cast(cast(NULL, 'nullable(string)'), 'nullable(enum8(\'Hello\' = 1))')));
