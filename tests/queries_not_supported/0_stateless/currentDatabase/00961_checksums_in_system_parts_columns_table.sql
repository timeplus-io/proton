DROP STREAM IF EXISTS test_00961;

create stream test_00961 (d date, a string, b uint8, x string, y int8, z uint32)
    ENGINE = MergeTree PARTITION BY d ORDER BY (a, b) SETTINGS index_granularity = 111, min_bytes_for_wide_part = 0;

INSERT INTO test_00961 VALUES ('2000-01-01', 'Hello, world!', 123, 'xxx yyy', -123, 123456789);

SELECT
    name,
    table,
    hash_of_all_files,
    hash_of_uncompressed_files,
    uncompressed_hash_of_compressed_files
FROM system.parts
WHERE table = 'test_00961' and database = currentDatabase();

DROP STREAM test_00961;

