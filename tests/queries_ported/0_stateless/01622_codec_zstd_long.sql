-- Tags: long

DROP STREAM IF EXISTS zstd_1_00;
DROP STREAM IF EXISTS zstd_1_24;
DROP STREAM IF EXISTS zstd_9_00;
DROP STREAM IF EXISTS zstd_9_24;
DROP STREAM IF EXISTS words;

CREATE STREAM words(i int, word string) ENGINE = Memory;
INSERT INTO words SELECT * FROM generateRandom('i int, word string',1,10) LIMIT 1 BY i LIMIT 10000;

CREATE STREAM zstd_1_00(n int, b string CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY n;
CREATE STREAM zstd_1_24(n int, b string CODEC(ZSTD(1,24))) ENGINE = MergeTree ORDER BY n;
CREATE STREAM zstd_9_00(n int, b string CODEC(ZSTD(9))) ENGINE = MergeTree ORDER BY n;
CREATE STREAM zstd_9_24(n int, b string CODEC(ZSTD(9,24))) ENGINE = MergeTree ORDER BY n;

INSERT INTO zstd_1_00 SELECT * FROM words;
INSERT INTO zstd_1_24 SELECT * FROM words;
INSERT INTO zstd_9_00 SELECT * FROM words;
INSERT INTO zstd_9_24 SELECT * FROM words;

SELECT count(n) FROM zstd_1_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT count(n) FROM zstd_1_24 LEFT JOIN words ON i == n WHERE b == word;
SELECT count(n) FROM zstd_9_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT count(n) FROM zstd_9_24 LEFT JOIN words ON i == n WHERE b == word;

DROP STREAM zstd_1_00;
DROP STREAM zstd_1_24;
DROP STREAM zstd_9_00;
DROP STREAM zstd_9_24;
DROP STREAM words;
