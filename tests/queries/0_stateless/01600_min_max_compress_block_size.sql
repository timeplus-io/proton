DROP STREAM IF EXISTS ms;

create stream ms (n int32) ENGINE = MergeTree() ORDER BY n SETTINGS min_compress_block_size = 1024, max_compress_block_size = 10240;

INSERT INTO ms SELECT * FROM numbers(1000);

SELECT COUNT(*) FROM ms;

DROP STREAM ms;
