DROP STREAM IF EXISTS test_buffer_table;

create stream test_buffer_table
(
    `a` int64
)
ENGINE = Buffer('', '', 1, 1000000, 1000000, 1000000, 1000000, 1000000, 1000000);

SELECT total_bytes FROM system.tables WHERE name = 'test_buffer_table' and database = currentDatabase();

INSERT INTO test_buffer_table SELECT number FROM numbers(1000);
SELECT total_bytes FROM system.tables WHERE name = 'test_buffer_table' and database = currentDatabase();

OPTIMIZE STREAM test_buffer_table;
SELECT total_bytes FROM system.tables WHERE name = 'test_buffer_table' and database = currentDatabase();

INSERT INTO test_buffer_table SELECT number FROM numbers(1000);
SELECT total_bytes FROM system.tables WHERE name = 'test_buffer_table' and database = currentDatabase();

OPTIMIZE STREAM test_buffer_table;
SELECT total_bytes FROM system.tables WHERE name = 'test_buffer_table' and database = currentDatabase();

DROP STREAM test_buffer_table;
