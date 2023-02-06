DROP STREAM IF EXISTS most_ordinary_mt;

CREATE STREAM most_ordinary_mt
(
   Key uint64
)
ENGINE = MergeTree()
ORDER BY tuple();

ALTER STREAM most_ordinary_mt RESET SETTING ttl; --{serverError 36}
ALTER STREAM most_ordinary_mt RESET SETTING allow_remote_fs_zero_copy_replication, xxx;  --{serverError 36}

DROP STREAM IF EXISTS most_ordinary_mt;
