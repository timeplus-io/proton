DROP STREAM IF EXISTS default_join1;
DROP STREAM IF EXISTS default_join2;

create stream default_join1(a int64, b int64) ENGINE=Memory;
create stream default_join2(a int64, b int64) ENGINE=Memory;

INSERT INTO default_join1 VALUES(1, 1), (2, 2), (3, 3);
INSERT INTO default_join2 VALUES(3, 3), (4, 4);

SELECT a, b FROM default_join1 JOIN (SELECT a, b FROM default_join2) as js2 USING a ORDER BY b SETTINGS join_default_strictness='ANY';

DROP STREAM default_join1;
DROP STREAM default_join2;
