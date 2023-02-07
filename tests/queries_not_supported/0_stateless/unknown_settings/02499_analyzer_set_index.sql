SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string,
    INDEX value_idx (value) TYPE set(1000) GRANULARITY 1
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table SELECT number, to_string(number) FROM numbers(10);

SELECT count() FROM test_table WHERE value = '1' SETTINGS force_data_skipping_indices = 'value_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value = '1' SETTINGS force_data_skipping_indices = 'value_idx';

DROP STREAM test_table;
