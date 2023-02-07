DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (key uint64, a uint64) ENGINE = Memory;
CREATE STREAM t2 (key uint64, a uint64) ENGINE = Join(ALL, RIGHT, key);

INSERT INTO t1 VALUES (1, 1), (2, 2);
INSERT INTO t2 VALUES (2, 2), (3, 3);

SET allow_experimental_analyzer = 0;
SELECT * FROM t1 ALL RIGHT JOIN t2 USING (key) ORDER BY key;

SET allow_experimental_analyzer = 1;
SELECT * FROM t1 ALL RIGHT JOIN t2 USING (key) ORDER BY key;
