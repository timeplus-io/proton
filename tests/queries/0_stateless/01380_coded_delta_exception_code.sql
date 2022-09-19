create stream delta_codec_synthetic (`id` Decimal(38, 10) CODEC(Delta, ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
create stream delta_codec_synthetic (`id` Decimal(38, 10) CODEC(DoubleDelta, ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
create stream delta_codec_synthetic (`id` Decimal(38, 10) CODEC(Gorilla, ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }

create stream delta_codec_synthetic (`id` uint64 CODEC(DoubleDelta(3), ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
create stream delta_codec_synthetic (`id` uint64 CODEC(Gorilla('hello, world'), ZSTD(22))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36 }
