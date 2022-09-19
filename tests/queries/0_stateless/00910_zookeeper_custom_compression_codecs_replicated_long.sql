-- Tags: long, replica

SET send_logs_level = 'fatal';
SET allow_suspicious_codecs = 1;

DROP STREAM IF EXISTS compression_codec_replicated1;
DROP STREAM IF EXISTS compression_codec_replicated2;

create stream compression_codec_replicated1(
    id uint64 CODEC(LZ4),
    data string CODEC(ZSTD),
    ddd date CODEC(NONE),
    somenum float64 CODEC(ZSTD(2)),
    somestr FixedString(3) CODEC(LZ4HC(7)),
    othernum int64 CODEC(Delta)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_replicated', '1') ORDER BY tuple();

create stream compression_codec_replicated2(
  id uint64 CODEC(LZ4),
  data string CODEC(ZSTD),
  ddd date CODEC(NONE),
  somenum float64 CODEC(ZSTD(2)),
  somestr FixedString(3) CODEC(LZ4HC(7)),
  othernum int64 CODEC(Delta)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_replicated', '2') ORDER BY tuple();


INSERT INTO compression_codec_replicated1 VALUES(1, 'hello', to_date('2018-12-14'), 1.1, 'aaa', 5);
INSERT INTO compression_codec_replicated1 VALUES(2, 'world', to_date('2018-12-15'), 2.2, 'bbb', 6);
INSERT INTO compression_codec_replicated1 VALUES(3, '!', to_date('2018-12-16'), 3.3, 'ccc', 7);

SYSTEM SYNC REPLICA compression_codec_replicated2;

SELECT * FROM compression_codec_replicated1 ORDER BY id;
SELECT * FROM compression_codec_replicated2 ORDER BY id;

OPTIMIZE STREAM compression_codec_replicated1 FINAL;

INSERT INTO compression_codec_replicated1 VALUES(2, '', to_date('2018-12-13'), 4.4, 'ddd', 8);

SYSTEM SYNC REPLICA compression_codec_replicated2;

DETACH TABLE compression_codec_replicated1;
ATTACH TABLE compression_codec_replicated1;

SELECT count(*) FROM compression_codec_replicated1 WHERE id = 2 GROUP BY id;
SELECT count(*) FROM compression_codec_replicated2 WHERE id = 2 GROUP BY id;

DROP STREAM IF EXISTS compression_codec_replicated1;
DROP STREAM IF EXISTS compression_codec_replicated2;

DROP STREAM IF EXISTS compression_codec_multiple_replicated1;
DROP STREAM IF EXISTS compression_codec_multiple_replicated2;

SET network_compression_method = 'lz4hc';

create stream compression_codec_multiple_replicated1 (
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC, Delta(4)),
    data string CODEC(ZSTD(2), NONE, Delta(2), LZ4HC, LZ4, LZ4, Delta(8)),
    ddd date CODEC(NONE, NONE, NONE, Delta(1), LZ4, ZSTD, LZ4HC, LZ4HC),
    somenum float64 CODEC(Delta(4), LZ4, LZ4, ZSTD(2), LZ4HC(5), ZSTD(3), ZSTD)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_multiple', '1') ORDER BY tuple();

create stream compression_codec_multiple_replicated2 (
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC, Delta(4)),
    data string CODEC(ZSTD(2), NONE, Delta(2), LZ4HC, LZ4, LZ4, Delta(8)),
    ddd date CODEC(NONE, NONE, NONE, Delta(1), LZ4, ZSTD, LZ4HC, LZ4HC),
    somenum float64 CODEC(Delta(4), LZ4, LZ4, ZSTD(2), LZ4HC(5), ZSTD(3), ZSTD)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_multiple', '2') ORDER BY tuple();


INSERT INTO compression_codec_multiple_replicated2 VALUES (1, 'world', to_date('2018-10-05'), 1.1), (2, 'hello', to_date('2018-10-01'), 2.2), (3, 'buy', to_date('2018-10-11'), 3.3);

SYSTEM SYNC REPLICA compression_codec_multiple_replicated1;

SELECT * FROM compression_codec_multiple_replicated2 ORDER BY id;
SELECT * FROM compression_codec_multiple_replicated1 ORDER BY id;

INSERT INTO compression_codec_multiple_replicated1 select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SYSTEM SYNC REPLICA compression_codec_multiple_replicated2;

SELECT count(*) FROM compression_codec_multiple_replicated1;
SELECT count(*) FROM compression_codec_multiple_replicated2;

SELECT count(distinct data) FROM compression_codec_multiple_replicated1;
SELECT count(distinct data) FROM compression_codec_multiple_replicated2;

SELECT floor(sum(somenum), 1) FROM compression_codec_multiple_replicated1;
SELECT floor(sum(somenum), 1) FROM compression_codec_multiple_replicated2;

TRUNCATE TABLE compression_codec_multiple_replicated1;
SYSTEM SYNC REPLICA compression_codec_multiple_replicated2;

INSERT INTO compression_codec_multiple_replicated1 select modulo(number, 100), to_string(number), to_date('2018-12-01'), 5.5 * number FROM system.numbers limit 10000;

SYSTEM SYNC REPLICA compression_codec_multiple_replicated2;

SELECT sum(cityHash64(*)) FROM compression_codec_multiple_replicated2;
SELECT sum(cityHash64(*)) FROM compression_codec_multiple_replicated1;

DROP STREAM IF EXISTS compression_codec_multiple_replicated1;
DROP STREAM IF EXISTS compression_codec_multiple_replicated2;

DROP STREAM IF EXISTS compression_codec_multiple_more_types_replicated;

create stream compression_codec_multiple_more_types_replicated (
    id Decimal128(13) CODEC(ZSTD, LZ4, ZSTD, ZSTD, Delta(2), Delta(4), Delta(1), LZ4HC),
    data FixedString(12) CODEC(ZSTD, ZSTD, Delta(1), Delta(1), Delta(1), NONE, NONE, NONE, LZ4HC),
    ddd nested (age uint8, Name string) CODEC(LZ4, LZ4HC, NONE, NONE, NONE, ZSTD, Delta(8))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_multiple_more_types_replicated', '1') ORDER BY tuple();

SHOW create stream compression_codec_multiple_more_types_replicated;

INSERT INTO compression_codec_multiple_more_types_replicated VALUES(1.5555555555555, 'hello world!', [77], ['John']);
INSERT INTO compression_codec_multiple_more_types_replicated VALUES(7.1, 'xxxxxxxxxxxx', [127], ['Henry']);

SELECT * FROM compression_codec_multiple_more_types_replicated order by id;

DROP STREAM IF EXISTS compression_codec_multiple_with_key_replicated;

SET network_compression_method = 'zstd';
SET network_zstd_compression_level = 5;

create stream compression_codec_multiple_with_key_replicated (
    somedate date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12), Delta, Delta),
    id uint64 CODEC(LZ4, ZSTD, Delta, NONE, LZ4HC, Delta),
    data string CODEC(ZSTD(2), Delta(1), LZ4HC, NONE, LZ4, LZ4)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00910/compression_codec_multiple_with_key_replicated', '1') PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;


INSERT INTO compression_codec_multiple_with_key_replicated VALUES(to_date('2018-10-12'), 100000, 'hello'), (to_date('2018-10-12'), 100002, 'world'), (to_date('2018-10-12'), 1111, '!');

SELECT data FROM compression_codec_multiple_with_key_replicated WHERE id BETWEEN 3 AND 1112;

INSERT INTO compression_codec_multiple_with_key_replicated SELECT to_date('2018-10-12'), number, to_string(number) FROM system.numbers LIMIT 1000;

SELECT COUNT(DISTINCT data) FROM compression_codec_multiple_with_key_replicated WHERE id < 222;

DROP STREAM IF EXISTS compression_codec_multiple_with_key_replicated;
DROP STREAM compression_codec_multiple_more_types_replicated;
