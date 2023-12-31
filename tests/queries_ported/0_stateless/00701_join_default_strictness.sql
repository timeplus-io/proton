DROP STREAM IF EXISTS a1;
DROP STREAM IF EXISTS a2;

 

create stream a1(a uint8, b uint8) ENGINE=Memory;
create stream a2(a uint8, b uint8) ENGINE=Memory;

INSERT INTO a1 VALUES (1, 1);
INSERT INTO a1 VALUES (1, 2);
INSERT INTO a1 VALUES (1, 3);
INSERT INTO a2 VALUES (1, 2);
INSERT INTO a2 VALUES (1, 3);
INSERT INTO a2 VALUES (1, 4);

SELECT a, b FROM a1 LEFT JOIN (SELECT a, b FROM a2) as js2 USING a ORDER BY b SETTINGS join_default_strictness='ANY';
SELECT a, b FROM a1 LEFT JOIN (SELECT a, b FROM a2) as js2 USING a ORDER BY b; -- default SETTINGS join_default_strictness='ALL';

DROP STREAM IF EXISTS a1;
DROP STREAM IF EXISTS a2;
