SELECT count() > 3 FROM (EXPLAIN PIPELINE header = 1 SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain LIKE '%Header: number uint64%';
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN SELECT * FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE '%Sort%';
SELECT count() > 0 FROM (EXPLAIN CURRENT TRANSACTION);
SELECT count() == 1 FROM (EXPLAIN SYNTAX SELECT number FROM system.numbers ORDER BY number DESC) WHERE explain ILIKE 'SELECT%';
SELECT trim(explain) == 'Asterisk' FROM (EXPLAIN AST SELECT * FROM system.numbers LIMIT 10) WHERE explain LIKE '%Asterisk%';

SELECT * FROM (
    EXPLAIN AST SELECT * FROM (
        EXPLAIN PLAN SELECT * FROM (
            EXPLAIN SYNTAX SELECT trim(explain) == 'Asterisk' FROM (
                EXPLAIN AST SELECT * FROM system.numbers LIMIT 10
            ) WHERE explain LIKE '%Asterisk%'
        )
    )
) FORMAT Null;

CREATE STREAM t1 ( a uint64 ) AS SELECT number AS a,now64() as _tp_time FROM system.numbers LIMIT 10000;
SELECT * FROM (SELECT sleep(3) AS n) AS a JOIN (SELECT 1 + sleep(3) AS f) AS b ON a.n == b.f; --- sleep to make sure that the stream t1 finishes being created and inserted into
SELECT rows > 1000 FROM (EXPLAIN ESTIMATE SELECT sum(a) FROM table(t1));
SELECT count() == 1 FROM (EXPLAIN ESTIMATE SELECT sum(a) FROM table(t1));

DROP STREAM IF EXISTS t1;

