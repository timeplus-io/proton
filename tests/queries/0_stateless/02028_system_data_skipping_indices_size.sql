DROP STREAM IF EXISTS test_table;

create stream test_table
(
    key uint64,
    value string,
    INDEX value_index value TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO test_table VALUES (0, 'Value');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

DROP STREAM test_table;
