DROP STREAM IF EXISTS t;
create stream t (d date, z uint32) ENGINE = MergeTree(d, (z), 1);

INSERT INTO t VALUES ('2017-01-01', 1);

WITH (d < '2018-01-01') AND (d < '2018-01-02') AS x
SELECT 1
FROM t
WHERE x;

DROP STREAM t;
