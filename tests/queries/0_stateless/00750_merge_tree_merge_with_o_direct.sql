DROP STREAM IF EXISTS sample_merge_tree;

create stream sample_merge_tree (dt DateTime, x uint64) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY x SETTINGS min_merge_bytes_to_use_direct_io=1, index_granularity = 8192;

INSERT INTO sample_merge_tree VALUES (to_datetime('2018-10-31 05:05:00'), 0), (to_datetime('2018-10-31 06:06:00'), 10), (to_datetime('2018-10-28 10:00:00'), 20);

OPTIMIZE STREAM sample_merge_tree FINAL;

SELECT * FROM sample_merge_tree ORDER BY x;

DROP STREAM IF EXISTS sample_merge_tree;
