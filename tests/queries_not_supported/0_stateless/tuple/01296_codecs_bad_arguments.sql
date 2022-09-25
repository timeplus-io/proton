DROP STREAM IF EXISTS delta_table;
DROP STREAM IF EXISTS zstd_table;
DROP STREAM IF EXISTS lz4_table;

create stream delta_table (`id` uint64 CODEC(Delta(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}
create stream zstd_table (`id` uint64 CODEC(ZSTD(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}
create stream lz4_table (`id` uint64 CODEC(LZ4HC(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 433}

create stream lz4_table (`id` uint64 CODEC(LZ4(tuple()))) ENGINE = MergeTree() ORDER BY tuple(); --{serverError 378}

SELECT 1;

DROP STREAM IF EXISTS delta_table;
DROP STREAM IF EXISTS zstd_table;
DROP STREAM IF EXISTS lz4_table;
