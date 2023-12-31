SET force_primary_key = 1;

DROP STREAM IF EXISTS samples;
CREATE STREAM samples (key uint32, value uint32) ENGINE = MergeTree() ORDER BY key PRIMARY KEY key;
INSERT INTO samples VALUES (1, 1)(2, 2)(3, 3)(4, 4)(5, 5);

-- all etries, verify that index is used
SELECT count() FROM samples WHERE key IN range(10);

-- some entries:
SELECT count() FROM samples WHERE key IN array_slice(range(100), 5, 10);

-- different type
SELECT count() FROM samples WHERE to_uint64(key) IN range(100);

SELECT 'empty:';
-- should be empty
SELECT count() FROM samples WHERE key IN array_slice(range(100), 10, 10);

-- not only ints:
SELECT 'a' IN split_by_char('c', 'abcdef');

SELECT 'errors:';
-- non-constant expressions in the right side of IN
SELECT count() FROM samples WHERE 1 IN range(samples.value); -- { serverError 47 }
SELECT count() FROM samples WHERE 1 IN range(rand() % 1000); -- { serverError 36 }

-- index is not used
SELECT count() FROM samples WHERE value IN range(3); -- { serverError 277 }

-- wrong type
SELECT 123 IN split_by_char('c', 'abcdef'); -- { serverError 53 }

DROP STREAM samples;