DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table (id uint64, value_ipv4 ipv4, value_ipv6 ipv6) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, '127.0.0.1', '127.0.0.1');

SELECT id FROM test_table WHERE value_ipv4 IN (SELECT value_ipv4 FROM test_table);
SELECT id FROM test_table WHERE value_ipv6 IN (SELECT value_ipv6 FROM test_table);

DROP STREAM test_table;
