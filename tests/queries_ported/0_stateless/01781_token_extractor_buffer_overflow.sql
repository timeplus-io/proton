SET max_block_size = 10, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 20;

DROP STREAM IF EXISTS bloom_filter;
CREATE STREAM bloom_filter (`id` uint64, `s` string, INDEX tok_bf (s, lower(s)) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO bloom_filter SELECT number, 'yyy,uuu' FROM numbers(1024);

SELECT max(id) FROM bloom_filter WHERE has_token(s, 'abc');
SELECT max(id) FROM bloom_filter WHERE has_token(s, 'abcabcabcabcabcabcabcab\0');

DROP STREAM bloom_filter;
