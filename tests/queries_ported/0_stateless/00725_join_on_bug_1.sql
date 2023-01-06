DROP STREAM IF EXISTS a1;
DROP STREAM IF EXISTS a2;

create stream a1(a uint8, b uint8) ENGINE=Memory;
create stream a2(a uint8, b uint8) ENGINE=Memory;

INSERT INTO a1 VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO a2 VALUES (1, 2), (1, 3), (1, 4);

SELECT * FROM a1 as a left JOIN a2 as b on a.a=b.a ORDER BY b SETTINGS join_default_strictness='ANY';
SELECT '-';
SELECT a1.*, a2.* FROM a1 ANY LEFT JOIN a2 USING a ORDER BY b;

DROP STREAM IF EXISTS a1;
DROP STREAM IF EXISTS a2;
