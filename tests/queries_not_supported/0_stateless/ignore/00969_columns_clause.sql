DROP STREAM IF EXISTS ColumnsClauseTest;

create stream ColumnsClauseTest (product_price int64, product_weight int16, amount int64) Engine=TinyLog;
INSERT INTO ColumnsClauseTest VALUES (100, 10, 324), (120, 8, 23);
SELECT COLUMNS('product.*') from ColumnsClauseTest ORDER BY product_price;

DROP STREAM ColumnsClauseTest;

SELECT number, COLUMNS('') FROM numbers(2);
SELECT number, COLUMNS('ber') FROM numbers(2); -- It works for unanchored regular expressions.
SELECT number, COLUMNS('x') FROM numbers(2);
SELECT COLUMNS('') FROM numbers(2);

SELECT COLUMNS('x') FROM numbers(10) WHERE number > 5; -- { serverError 51 }

SELECT * FROM numbers(2) WHERE NOT ignore();
SELECT * FROM numbers(2) WHERE NOT ignore(*);
SELECT * FROM numbers(2) WHERE NOT ignore(COLUMNS('.+'));
SELECT * FROM numbers(2) WHERE NOT ignore(COLUMNS('x'));
SELECT COLUMNS('n') + COLUMNS('u') FROM system.numbers LIMIT 2;

SELECT COLUMNS('n') + COLUMNS('u') FROM (SELECT 1 AS a, 2 AS b); -- { serverError 42 }
SELECT COLUMNS('a') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('a') + COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('b') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('a|b') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b); -- { serverError 42 }
SELECT plus(COLUMNS('^(a|b)$')) FROM (SELECT 1 AS a, 2 AS b);
