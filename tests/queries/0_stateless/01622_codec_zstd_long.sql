-- Tags: long

DROP STREAM IF EXISTS zstd_1_00;
DROP STREAM IF EXISTS zstd_1_24;
DROP STREAM IF EXISTS zstd_9_00;
DROP STREAM IF EXISTS zstd_9_24;
DROP STREAM IF EXISTS words;

create stream words(i int, word string) ;
INSERT INTO words SELECT * FROM generateRandom('i int, word string',1,10) LIMIT 10000;

create stream zstd_1_00(n int, b string CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY n;
create stream zstd_1_24(n int, b string CODEC(ZSTD(1,24))) ENGINE = MergeTree ORDER BY n;
create stream zstd_9_00(n int, b string CODEC(ZSTD(9))) ENGINE = MergeTree ORDER BY n;
create stream zstd_9_24(n int, b string CODEC(ZSTD(9,24))) ENGINE = MergeTree ORDER BY n;

INSERT INTO zstd_1_00 SELECT * FROM words;
INSERT INTO zstd_1_24 SELECT * FROM words;
INSERT INTO zstd_9_00 SELECT * FROM words;
INSERT INTO zstd_9_24 SELECT * FROM words;

SELECT COUNT(n) FROM zstd_1_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_1_24 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_9_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_9_24 LEFT JOIN words ON i == n WHERE b == word;

DROP STREAM zstd_1_00;
DROP STREAM zstd_1_24;
DROP STREAM zstd_9_00;
DROP STREAM zstd_9_24;
DROP STREAM words;
