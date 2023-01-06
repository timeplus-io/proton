SET query_mode='table';
DROP STREAM IF EXISTS t1_00856;
DROP STREAM IF EXISTS t2_00856;
create stream t1_00856 (n int32) ;
create stream t2_00856 (a int32, n int32) ;

SELECT count() FROM t1_00856 WHERE if(1, 1, n = 0);
SELECT count(n) FROM t2_00856 WHERE if(1, 1, n = 0);
SELECT count() FROM t2_00856 WHERE if(1, 1, n = 0);

DROP STREAM t1_00856;
DROP STREAM t2_00856;
