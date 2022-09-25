DROP STREAM IF EXISTS alter_index_test;

create stream alter_index_test (
    a uint32,
    b date,
    c uint32,
    d uint32,
    INDEX index_a a TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY tuple();

SELECT * FROM system.data_skipping_indices WHERE table = 'alter_index_test' AND database = currentDatabase();

ALTER STREAM alter_index_test ADD INDEX index_b b type minmax granularity 1 FIRST;

ALTER STREAM alter_index_test ADD INDEX index_c c type set(0) granularity 2 AFTER index_b;

ALTER STREAM alter_index_test ADD INDEX index_d d type set(0) granularity 1;

SELECT * FROM system.data_skipping_indices WHERE table = 'alter_index_test' AND database = currentDatabase();

DETACH STREAM alter_index_test;
ATTACH STREAM alter_index_test;

SELECT * FROM system.data_skipping_indices WHERE table = 'alter_index_test' AND database = currentDatabase();

DROP STREAM IF EXISTS alter_index_test;
