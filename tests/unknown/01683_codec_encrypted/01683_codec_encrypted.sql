-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

DROP STREAM IF EXISTS encryption_test;
CREATE STREAM encryption_test (i int, s string Codec(AES_128_GCM_SIV)) ENGINE = MergeTree ORDER BY i;

INSERT INTO encryption_test VALUES (1, 'Some plaintext');
SELECT * FROM encryption_test;

DROP STREAM encryption_test;

CREATE STREAM encryption_test (i int, s string Codec(AES_256_GCM_SIV)) ENGINE = MergeTree ORDER BY i;

INSERT INTO encryption_test VALUES (1, 'Some plaintext');
SELECT * FROM encryption_test;

DROP STREAM encryption_test;
