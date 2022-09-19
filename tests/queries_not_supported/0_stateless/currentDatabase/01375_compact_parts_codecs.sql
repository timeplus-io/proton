-- Tags: no-parallel

DROP STREAM IF EXISTS codecs;

create stream codecs (id uint32, val uint32, s string) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, to_string(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP STREAM codecs;

create stream codecs (id uint32 CODEC(NONE), val uint32 CODEC(NONE), s string CODEC(NONE)) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, to_string(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP STREAM codecs;

create stream codecs (id uint32, val uint32 CODEC(Delta, ZSTD), s string CODEC(ZSTD)) 
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 10000;
INSERT INTO codecs SELECT number, number, to_string(number) FROM numbers(1000);
SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) 
    FROM system.parts 
    WHERE table = 'codecs' AND database = currentDatabase();

SELECT sum(id), sum(val), max(s) FROM codecs;

DETACH TABLE codecs;
ATTACH table codecs;

SELECT sum(id), sum(val), max(s) FROM codecs;

DROP STREAM codecs;
