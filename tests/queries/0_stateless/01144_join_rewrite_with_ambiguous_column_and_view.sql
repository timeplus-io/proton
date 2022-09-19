DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS view1;

create stream t1 (id uint32, value1 string) ENGINE MergeTree() ORDER BY id;
create stream t2 (id uint32, value2 string) ENGINE MergeTree() ORDER BY id;
create stream t3 (id uint32, value3 string) ENGINE MergeTree() ORDER BY id;

INSERT INTO t1 (id, value1) VALUES (1, 'val11');
INSERT INTO t2 (id, value2) VALUES (1, 'val21');
INSERT INTO t3 (id, value3) VALUES (1, 'val31');

SET enable_optimize_predicate_expression = 1;

SELECT t1.id, t2.id as id, t3.id as value
FROM (select number as id, 42 as value from numbers(4)) t1
LEFT JOIN (select number as id, 42 as value from numbers(3)) t2 ON t1.id = t2.id
LEFT JOIN (select number as id, 42 as value from numbers(2)) t3 ON t1.id = t3.id
WHERE id > 0 AND value < 42;

CREATE VIEW IF NOT EXISTS view1 AS
    SELECT t1.id AS id, t1.value1 AS value1, t2.value2 AS value2, t3.value3 AS value3
    FROM t1
    LEFT JOIN t2 ON t1.id = t2.id
    LEFT JOIN t3 ON t1.id = t3.id
    WHERE t1.id > 0;

SELECT * FROM view1 WHERE id = 1;

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS view1;
