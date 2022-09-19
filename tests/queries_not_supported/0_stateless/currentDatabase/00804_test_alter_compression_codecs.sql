-- Tags: no-parallel

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS alter_compression_codec;

create stream alter_compression_codec (
    somedate date CODEC(LZ4),
    id uint64 CODEC(NONE)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id;

INSERT INTO alter_compression_codec VALUES('2018-01-01', 1);
INSERT INTO alter_compression_codec VALUES('2018-01-01', 2);
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER STREAM alter_compression_codec ADD COLUMN alter_column string DEFAULT 'default_value' CODEC(ZSTD);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 3, '3');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 4, '4');
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER STREAM alter_compression_codec MODIFY COLUMN alter_column CODEC(NONE);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 5, '5');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 6, '6');
SELECT * FROM alter_compression_codec ORDER BY id;

OPTIMIZE STREAM alter_compression_codec FINAL;
SELECT * FROM alter_compression_codec ORDER BY id;

SET allow_suspicious_codecs = 1;
ALTER STREAM alter_compression_codec MODIFY COLUMN alter_column CODEC(ZSTD, LZ4HC, LZ4, LZ4, NONE);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';

INSERT INTO alter_compression_codec VALUES('2018-01-01', 7, '7');
INSERT INTO alter_compression_codec VALUES('2018-01-01', 8, '8');
OPTIMIZE STREAM alter_compression_codec FINAL;
SELECT * FROM alter_compression_codec ORDER BY id;

ALTER STREAM alter_compression_codec MODIFY COLUMN alter_column FixedString(100);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'alter_compression_codec' AND name = 'alter_column';


DROP STREAM IF EXISTS alter_compression_codec;

DROP STREAM IF EXISTS alter_bad_codec;

create stream alter_bad_codec (
    somedate date CODEC(LZ4),
    id uint64 CODEC(NONE)
) ENGINE = MergeTree() ORDER BY tuple();

ALTER STREAM alter_bad_codec ADD COLUMN alter_column DateTime DEFAULT '2019-01-01 00:00:00' CODEC(gbdgkjsdh); -- { serverError 432 }

ALTER STREAM alter_bad_codec ADD COLUMN alter_column DateTime DEFAULT '2019-01-01 00:00:00' CODEC(ZSTD(100)); -- { serverError 433 }

DROP STREAM IF EXISTS alter_bad_codec;

DROP STREAM IF EXISTS large_alter_table_00804;
DROP STREAM IF EXISTS store_of_hash_00804;

create stream large_alter_table_00804 (
    somedate date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
    id uint64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
    data string CODEC(ZSTD(2), LZ4HC, NONE, LZ4, LZ4)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO large_alter_table_00804 SELECT to_date('2019-01-01'), number, to_string(number + rand()) FROM system.numbers LIMIT 300000;

create stream store_of_hash_00804 (hash uint64) ();

INSERT INTO store_of_hash_00804 SELECT sum(cityHash64(*)) FROM large_alter_table_00804;

ALTER STREAM large_alter_table_00804 MODIFY COLUMN data CODEC(NONE, LZ4, LZ4HC, ZSTD);

OPTIMIZE STREAM large_alter_table_00804;

SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'large_alter_table_00804' AND name = 'data';

DETACH TABLE large_alter_table_00804;
ATTACH TABLE large_alter_table_00804;

INSERT INTO store_of_hash_00804 SELECT sum(cityHash64(*)) FROM large_alter_table_00804;

SELECT COUNT(hash) FROM store_of_hash_00804;
SELECT COUNT(DISTINCT hash) FROM store_of_hash_00804;

DROP STREAM IF EXISTS large_alter_table_00804;
DROP STREAM IF EXISTS store_of_hash_00804;
