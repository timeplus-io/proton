SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    id uint64,
    value string
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT * FROM test_table) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT 1 UNION DISTINCT SELECT 1) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT * FROM test_table UNION DISTINCT SELECT * FROM test_table) SELECT * FROM cte_subquery;

DROP STREAM test_table;
