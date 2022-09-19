-- Tags: no-parallel

SET send_logs_level = 'fatal';
SET allow_suspicious_codecs = 1;

-- copy-paste for storage log

DROP STREAM IF EXISTS compression_codec_log;

create stream compression_codec_log(
    id uint64 CODEC(LZ4),
    data string CODEC(ZSTD),
    ddd date CODEC(NONE),
    somenum float64 CODEC(ZSTD(2)),
    somestr FixedString(3) CODEC(LZ4HC(7)),
    othernum int64 CODEC(Delta)
)  ();

SHOW create stream compression_codec_log;

INSERT INTO compression_codec_log VALUES(1, 'hello', to_date('2018-12-14'), 1.1, 'aaa', 5);
INSERT INTO compression_codec_log VALUES(2, 'world', to_date('2018-12-15'), 2.2, 'bbb', 6);
INSERT INTO compression_codec_log VALUES(3, '!', to_date('2018-12-16'), 3.3, 'ccc', 7);

SELECT * FROM compression_codec_log ORDER BY id;

INSERT INTO compression_codec_log VALUES(2, '', to_date('2018-12-13'), 4.4, 'ddd', 8);

DETACH TABLE compression_codec_log;
ATTACH TABLE compression_codec_log;

SELECT count(*) FROM compression_codec_log WHERE id = 2 GROUP BY id;

DROP STREAM IF EXISTS compression_codec_log;

DROP STREAM IF EXISTS compression_codec_multiple_log;

create stream compression_codec_multiple_log (
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC, Delta(4)),
    data string CODEC(ZSTD(2), NONE, Delta(2), LZ4HC, LZ4, LZ4, Delta(8)),
    ddd date CODEC(NONE, NONE, NONE, Delta(1), LZ4, ZSTD, LZ4HC, LZ4HC),
    somenum float64 CODEC(Delta(4), LZ4, LZ4, ZSTD(2), LZ4HC(5), ZSTD(3), ZSTD)
)  ();

SHOW create stream compression_codec_multiple_log;

INSERT INTO compression_codec_multiple_log VALUES (1, 'world', to_date('2018-10-05'), 1.1), (2, 'hello', to_date('2018-10-01'), 2.2), (3, 'buy', to_date('2018-10-11'), 3.3);

SELECT * FROM compression_codec_multiple_log ORDER BY id;

INSERT INTO compression_codec_multiple_log select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SELECT count(*) FROM compression_codec_multiple_log;

SELECT count(distinct data) FROM compression_codec_multiple_log;

SELECT floor(sum(somenum), 1) FROM compression_codec_multiple_log;

TRUNCATE TABLE compression_codec_multiple_log;

INSERT INTO compression_codec_multiple_log select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SELECT sum(cityHash64(*)) FROM compression_codec_multiple_log;

-- copy-paste for storage tiny log
DROP STREAM IF EXISTS compression_codec_tiny_log;

create stream compression_codec_tiny_log(
    id uint64 CODEC(LZ4),
    data string CODEC(ZSTD),
    ddd date CODEC(NONE),
    somenum float64 CODEC(ZSTD(2)),
    somestr FixedString(3) CODEC(LZ4HC(7)),
    othernum int64 CODEC(Delta)
) ();

SHOW create stream compression_codec_tiny_log;

INSERT INTO compression_codec_tiny_log VALUES(1, 'hello', to_date('2018-12-14'), 1.1, 'aaa', 5);
INSERT INTO compression_codec_tiny_log VALUES(2, 'world', to_date('2018-12-15'), 2.2, 'bbb', 6);
INSERT INTO compression_codec_tiny_log VALUES(3, '!', to_date('2018-12-16'), 3.3, 'ccc', 7);

SELECT * FROM compression_codec_tiny_log ORDER BY id;

INSERT INTO compression_codec_tiny_log VALUES(2, '', to_date('2018-12-13'), 4.4, 'ddd', 8);

DETACH TABLE compression_codec_tiny_log;
ATTACH TABLE compression_codec_tiny_log;

SELECT count(*) FROM compression_codec_tiny_log WHERE id = 2 GROUP BY id;

DROP STREAM IF EXISTS compression_codec_tiny_log;

DROP STREAM IF EXISTS compression_codec_multiple_tiny_log;

create stream compression_codec_multiple_tiny_log (
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC, Delta(4)),
    data string CODEC(ZSTD(2), NONE, Delta(2), LZ4HC, LZ4, LZ4, Delta(8)),
    ddd date CODEC(NONE, NONE, NONE, Delta(1), LZ4, ZSTD, LZ4HC, LZ4HC),
    somenum float64 CODEC(Delta(4), LZ4, LZ4, ZSTD(2), LZ4HC(5), ZSTD(3), ZSTD)
) ();

SHOW create stream compression_codec_multiple_tiny_log;

INSERT INTO compression_codec_multiple_tiny_log VALUES (1, 'world', to_date('2018-10-05'), 1.1), (2, 'hello', to_date('2018-10-01'), 2.2), (3, 'buy', to_date('2018-10-11'), 3.3);

SELECT * FROM compression_codec_multiple_tiny_log ORDER BY id;

INSERT INTO compression_codec_multiple_tiny_log select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SELECT count(*) FROM compression_codec_multiple_tiny_log;

SELECT count(distinct data) FROM compression_codec_multiple_tiny_log;

SELECT floor(sum(somenum), 1) FROM compression_codec_multiple_tiny_log;

TRUNCATE TABLE compression_codec_multiple_tiny_log;

INSERT INTO compression_codec_multiple_tiny_log select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SELECT sum(cityHash64(*)) FROM compression_codec_multiple_tiny_log;

DROP STREAM compression_codec_multiple_log;
DROP STREAM compression_codec_multiple_tiny_log;
