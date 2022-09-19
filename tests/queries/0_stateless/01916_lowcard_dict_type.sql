DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

create stream t1 (`x` uint32, `lc` LowCardinality(string) ) ;
INSERT INTO t1 VALUES (1, '1'), (2, '2');

SELECT toIntervalMinute(lc) as e, to_type_name(e) FROM t1;
SELECT toIntervalDay(lc) as e, to_type_name(e) FROM t1;

create stream t2 (`x` uint32, `lc` LowCardinality(string) ) ;
INSERT INTO t2 VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba2');

SELECT toUUID(lc) as e, to_type_name(e) FROM t2;

INSERT INTO t2 VALUES (2, '2');

SELECT toIntervalMinute(lc), to_type_name(materialize(r.lc)) FROM t1 AS l INNER JOIN t2 as r USING (lc);

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
