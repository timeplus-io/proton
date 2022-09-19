DROP STREAM IF EXISTS numbers_squashed;
create stream numbers_squashed (number uint8) ENGINE = StripeLog;

SET min_insert_block_size_rows = 100;
SET min_insert_block_size_bytes = 0;
SET max_threads = 1;

INSERT INTO numbers_squashed
SELECT array_join(range(10)) AS number
UNION ALL
SELECT array_join(range(100))
UNION ALL
SELECT array_join(range(10));

SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM numbers_squashed;

INSERT INTO numbers_squashed
SELECT array_join(range(100)) AS number
UNION ALL
SELECT array_join(range(10))
UNION ALL
SELECT array_join(range(100));

SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM numbers_squashed;

INSERT INTO numbers_squashed
SELECT array_join(range(10)) AS number
UNION ALL
SELECT array_join(range(100))
UNION ALL
SELECT array_join(range(100));

SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM numbers_squashed;

INSERT INTO numbers_squashed
SELECT array_join(range(10)) AS number
UNION ALL
SELECT array_join(range(10))
UNION ALL
SELECT array_join(range(10))
UNION ALL
SELECT array_join(range(100))
UNION ALL
SELECT array_join(range(10));

SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM numbers_squashed;

SET min_insert_block_size_rows = 10;

INSERT INTO numbers_squashed
SELECT array_join(range(10)) AS number
UNION ALL
SELECT array_join(range(10))
UNION ALL
SELECT array_join(range(10))
UNION ALL
SELECT array_join(range(100))
UNION ALL
SELECT array_join(range(10));

SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM numbers_squashed;

DROP STREAM numbers_squashed;
