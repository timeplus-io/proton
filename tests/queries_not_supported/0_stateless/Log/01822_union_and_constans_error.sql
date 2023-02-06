drop stream if exists t0;
CREATE STREAM t0 (c0 string) ENGINE = Log();

SELECT is_null(t0.c0) OR count('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING is_null(t0.c0)
UNION ALL
SELECT is_null(t0.c0) OR count('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING NOT is_null(t0.c0)
UNION ALL
SELECT is_null(t0.c0) OR count('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING is_null(is_null(t0.c0))
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

drop stream if exists t0;
