DROP STREAM IF EXISTS ms;

CREATE STREAM ms (n int32) ENGINE = MergeTree() ORDER BY n SETTINGS min_compress_block_size = 1024, max_compress_block_size = 10240;

INSERT INTO ms SELECT * FROM numbers(1000);

SELECT count(*) FROM ms;

DROP STREAM ms;
