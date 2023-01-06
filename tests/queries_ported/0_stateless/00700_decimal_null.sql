SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS decimal;

create stream IF NOT EXISTS decimal
(
    a decimal(9, 2),
    b decimal(18, 5),
    c decimal(38, 5),
    d nullable(decimal(9, 4)),
    e nullable(decimal(18, 8)),
    f nullable(decimal(38, 8))
) ;

SELECT to_nullable(to_decimal32(32, 0)) AS x, assume_not_null(x);
SELECT to_nullable(to_decimal64(64, 0)) AS x, assume_not_null(x);
SELECT to_nullable(to_decimal128(128, 0)) AS x, assume_not_null(x);

SELECT if_null(to_decimal32(1, 0), NULL), if_null(to_decimal64(1, 0), NULL), if_null(to_decimal128(1, 0), NULL);
SELECT if_null(to_nullable(to_decimal32(2, 0)), NULL), if_null(to_nullable(to_decimal64(2, 0)), NULL), if_null(to_nullable(to_decimal128(2, 0)), NULL);
SELECT if_null(NULL, to_decimal32(3, 0)), if_null(NULL, to_decimal64(3, 0)), if_null(NULL, to_decimal128(3, 0));
SELECT if_null(NULL, to_nullable(to_decimal32(4, 0))), if_null(NULL, to_nullable(to_decimal64(4, 0))), if_null(NULL, to_nullable(to_decimal128(4, 0)));

SELECT coalesce(to_decimal32(5, 0), NULL), coalesce(to_decimal64(5, 0), NULL), coalesce(to_decimal128(5, 0), NULL);
SELECT coalesce(NULL, to_decimal32(6, 0)), coalesce(NULL, to_decimal64(6, 0)), coalesce(NULL, to_decimal128(6, 0));

SELECT coalesce(to_nullable(to_decimal32(7, 0)), NULL), coalesce(to_nullable(to_decimal64(7, 0)), NULL), coalesce(to_nullable(to_decimal128(7, 0)), NULL);
SELECT coalesce(NULL, to_nullable(to_decimal32(8, 0))), coalesce(NULL, to_nullable(to_decimal64(8, 0))), coalesce(NULL, to_nullable(to_decimal128(8, 0)));

SELECT null_if(to_nullable(to_decimal32(1, 0)), to_decimal32(1, 0)), null_if(to_nullable(to_decimal64(1, 0)), to_decimal64(1, 0));
SELECT null_if(to_decimal32(1, 0), to_nullable(to_decimal32(1, 0))), null_if(to_decimal64(1, 0), to_nullable(to_decimal64(1, 0)));
SELECT null_if(to_nullable(to_decimal32(1, 0)), to_decimal32(2, 0)), null_if(to_nullable(to_decimal64(1, 0)), to_decimal64(2, 0));
SELECT null_if(to_decimal32(1, 0), to_nullable(to_decimal32(2, 0))), null_if(to_decimal64(1, 0), to_nullable(to_decimal64(2, 0)));
SELECT null_if(to_nullable(to_decimal128(1, 0)), to_decimal128(1, 0));
SELECT null_if(to_decimal128(1, 0), to_nullable(to_decimal128(1, 0)));
SELECT null_if(to_nullable(to_decimal128(1, 0)), to_decimal128(2, 0));
SELECT null_if(to_decimal128(1, 0), to_nullable(to_decimal128(2, 0)));

INSERT INTO decimal (a, b, c, d, e, f) VALUES (1.1, 1.1, 1.1, 1.1, 1.1, 1.1);
INSERT INTO decimal (a, b, c, d) VALUES (2.2, 2.2, 2.2, 2.2);
INSERT INTO decimal (a, b, c, e) VALUES (3.3, 3.3, 3.3, 3.3);
INSERT INTO decimal (a, b, c, f) VALUES (4.4, 4.4, 4.4, 4.4);
INSERT INTO decimal (a, b, c) VALUES (5.5, 5.5, 5.5);

SELECT sleep(3);

SELECT * FROM decimal ORDER BY d, e, f;
SELECT is_null(a), is_not_null(a) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT is_null(b), is_not_null(b) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT is_null(c), is_not_null(c) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT is_null(d), is_not_null(d) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT is_null(e), is_not_null(e) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT is_null(f), is_not_null(f) FROM decimal WHERE a = to_decimal32(5.5, 1);
SELECT count() FROM decimal WHERE a IS NOT NULL;
SELECT count() FROM decimal WHERE b IS NOT NULL;
SELECT count() FROM decimal WHERE c IS NOT NULL;
SELECT count() FROM decimal WHERE d IS NULL;
SELECT count() FROM decimal WHERE e IS NULL;
SELECT count() FROM decimal WHERE f IS NULL;
SELECT count() FROM decimal WHERE d IS NULL AND e IS NULL;
SELECT count() FROM decimal WHERE d IS NULL AND f IS NULL;
SELECT count() FROM decimal WHERE e IS NULL AND f IS NULL;

DROP STREAM IF EXISTS decimal;
