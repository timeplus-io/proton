SET query_mode='table';
DROP STREAM IF EXISTS cube;
create stream cube(a string, b int32, s int32) ;

INSERT INTO cube(a,b,s) VALUES ('a', 1, 10), ('a', 1, 15), ('a', 2, 20);
INSERT INTO cube(a,b,s) VALUES ('a', 2, 25), ('b', 1, 10), ('b', 1, 5);
INSERT INTO cube(a,b,s) VALUES ('b', 2, 20), ('b', 2, 15);

SELECT sleep(3);
SELECT a, b, sum(s), count() from cube GROUP BY CUBE(a, b) ORDER BY a, b;

SELECT a, b, sum(s), count() from cube GROUP BY CUBE(a, b) WITH TOTALS ORDER BY a, b;

SELECT a, b, sum(s), count() from cube GROUP BY a, b WITH CUBE ORDER BY a, b;

SELECT a, b, sum(s), count() from cube GROUP BY a, b WITH CUBE WITH TOTALS ORDER BY a, b;

SET group_by_two_level_threshold = 1;
SELECT a, b, sum(s), count() from cube GROUP BY a, b WITH CUBE ORDER BY a, b;

DROP STREAM cube;
