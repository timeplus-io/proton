DROP STREAM IF EXISTS retention_test;

create stream retention_test(date date, uid int32);
INSERT INTO retention_test(date,uid) SELECT '2018-08-06', number FROM numbers(80);
INSERT INTO retention_test(date,uid) SELECT '2018-08-07', number FROM numbers(50);
INSERT INTO retention_test(date,uid) SELECT '2018-08-08', number FROM numbers(60);

SELECT sum(r[1]) as r1, sum(r[2]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-07') GROUP BY uid);
SELECT sum(r[1]) as r1, sum(r[2]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-08') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-08') GROUP BY uid);
SELECT sum(r[1]) as r1, sum(r[2]) as r2, sum(r[3]) as r3 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07', date = '2018-08-08') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-07', '2018-08-08') GROUP BY uid);

DROP STREAM retention_test;
