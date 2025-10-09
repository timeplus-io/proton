SELECT i FROM generate_random('i array(nullable(enum8(\'hello\' = 1, \'world\' = 5)))', 1025, 65535, 9223372036854775807) LIMIT 10; -- { serverError 128 }
