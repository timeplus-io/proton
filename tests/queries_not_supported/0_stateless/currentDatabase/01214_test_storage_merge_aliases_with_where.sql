DROP STREAM IF EXISTS tt1;
DROP STREAM IF EXISTS tt2;
DROP STREAM IF EXISTS tt3;
DROP STREAM IF EXISTS tt4;
DROP STREAM IF EXISTS tt_m;

create stream tt1 (a uint32, b uint32 ALIAS a) ;
create stream tt2 (a uint32, b uint32 ALIAS a * 2) ;
create stream tt3 (a uint32, b uint32 ALIAS c, c uint32) ;
create stream tt4 (a uint32, b uint32 ALIAS 12) ;
create stream tt_m (a uint32, b uint32) ENGINE = Merge(currentDatabase(), 'tt1|tt2|tt3|tt4');

INSERT INTO tt1 VALUES (1);
INSERT INTO tt2 VALUES (2);
INSERT INTO tt3(a, c) VALUES (3, 4);
INSERT INTO tt4 VALUES (5);

-- { echo  }
SELECT * FROM tt_m order by a;
SELECT * FROM tt_m WHERE b != 0 order by b;
SELECT * FROM tt_m WHERE b != 1 order by b;
SELECT * FROM tt_m WHERE b != a * 2 order by b;
SELECT * FROM tt_m WHERE b / 2 != a order by b;

SELECT b FROM tt_m WHERE b >= 0 order by b;
SELECT b FROM tt_m WHERE b == 12;
SELECT b FROM tt_m ORDER BY b;
SELECT b, count()  FROM tt_m GROUP BY b order by b;
SELECT b FROM tt_m  order by b LIMIT 1 BY b;

SELECT a FROM tt_m WHERE b = 12;
SELECT max(a) FROM tt_m group by b order by b;
SELECT a FROM tt_m order by b LIMIT 1 BY b;

