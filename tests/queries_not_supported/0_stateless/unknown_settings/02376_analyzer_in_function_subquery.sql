SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value_0'), (1, 'Value_1'), (2, 'Value_2');

DROP STREAM IF EXISTS test_table_for_in;
CREATE STREAM test_table_for_in
(
    id uint64
) ENGINE=TinyLog;

INSERT INTO test_table_for_in VALUES (0), (1);

-- { echoOn }

SELECT id, value FROM test_table WHERE 1 IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE 0 IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT 2);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN test_table_for_in;

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT id FROM test_table_for_in);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT id FROM test_table_for_in UNION DISTINCT SELECT id FROM test_table_for_in);

SELECT '--';

WITH cte_test_table_for_in AS (SELECT id FROM test_table_for_in) SELECT id, value FROM test_table WHERE id IN cte_test_table_for_in;

SELECT '--';

WITH cte_test_table_for_in AS (SELECT id FROM test_table_for_in) SELECT id, value
FROM test_table WHERE id IN (SELECT id FROM cte_test_table_for_in UNION DISTINCT SELECT id FROM cte_test_table_for_in);

-- { echoOff }

DROP STREAM test_table;
DROP STREAM test_table_for_in;
