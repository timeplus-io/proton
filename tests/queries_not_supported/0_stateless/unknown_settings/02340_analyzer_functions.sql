SET allow_experimental_analyzer = 1;

DESCRIBE (SELECT 1 + 1);
SELECT 1 + 1;

SELECT '--';

DESCRIBE (SELECT dummy + dummy);
SELECT dummy + dummy;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT '--';

DESCRIBE (SELECT id + length(value) FROM test_table);
SELECT id + length(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT concat(concat(to_string(id), '_'), (value)) FROM test_table);
SELECT concat(concat(to_string(id), '_'), (value)) FROM test_table;
