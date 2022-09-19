DROP STREAM IF EXISTS compression_codec_on_alias;

select 'create stream compression_codec_on_alias with CODEC on ALIAS type';

create stream compression_codec_on_alias (
    `c0` ALIAS c1 CODEC(ZSTD),
    c1 uint64
) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c1; -- { serverError 36 }

select 'create stream compression_codec_on_alias with proper CODEC';

create stream compression_codec_on_alias (
    c0 uint64 CODEC(ZSTD),
    c1 uint64
) ENGINE = MergeTree() PARTITION BY c0 ORDER BY c1; -- success

select 'alter stream compression_codec_on_alias add column (ALIAS type) with CODEC';

ALTER STREAM compression_codec_on_alias ADD COLUMN `c3` ALIAS c2 CODEC(ZSTD) AFTER c2; -- { serverError 36 }

select 'alter stream compression_codec_on_alias add column (NOT ALIAS type) with CODEC';

ALTER STREAM compression_codec_on_alias ADD COLUMN c2 uint64 CODEC(ZSTD) AFTER c1; -- success

DROP STREAM IF EXISTS compression_codec_on_alias;
