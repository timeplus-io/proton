DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

create stream t1 (`id` int32, `key` string) ;
create stream t2 (`id` int32, `key` string) ;

INSERT INTO t1 VALUES (0, '');
INSERT INTO t2 VALUES (0, '');

SELECT * FROM t1 ANY INNER JOIN t2 ON ((NULL = t1.key) = t2.id) AND (('' = t1.key) = t2.id);

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
