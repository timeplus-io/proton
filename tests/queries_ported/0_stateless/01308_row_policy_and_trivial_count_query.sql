SET optimize_move_to_prewhere = 1;

DROP STREAM IF EXISTS t;

CREATE STREAM t (x uint8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2), (3);

SELECT count() FROM t;
DROP ROW POLICY IF EXISTS filter ON t;
CREATE ROW POLICY filter ON t USING (x % 2 = 1) TO ALL;
SELECT count() FROM t;
DROP ROW POLICY filter ON t;
SELECT count() FROM t;

DROP STREAM t;
