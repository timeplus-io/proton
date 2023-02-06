DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (`x` uint32, `lc` low_cardinality(string) ) ENGINE = Memory;
INSERT INTO t1 VALUES (1, '1'), (2, '2');

SELECT to_interval_minute(lc) as e, to_type_name(e) FROM t1;
SELECT to_interval_day(lc) as e, to_type_name(e) FROM t1;

CREATE STREAM t2 (`x` uint32, `lc` low_cardinality(string) ) ENGINE = Memory;
INSERT INTO t2 VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba2');

SELECT to_uuid(lc) as e, to_type_name(e) FROM t2;

INSERT INTO t2 VALUES (2, '2');

SELECT to_interval_minute(lc), to_type_name(materialize(r.lc)) FROM t1 AS l INNER JOIN t2 as r USING (lc);

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
