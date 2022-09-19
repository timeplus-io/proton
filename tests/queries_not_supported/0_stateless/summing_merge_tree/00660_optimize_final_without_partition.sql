-- Tags: not_supported, blocked_by_SummingMergeTree
SET optimize_on_insert = 0;

DROP STREAM IF EXISTS partitioned_by_tuple;

create stream partitioned_by_tuple (d date, x uint8, w string, y uint8) ENGINE SummingMergeTree (y) PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO partitioned_by_tuple VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO partitioned_by_tuple VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO partitioned_by_tuple VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE STREAM partitioned_by_tuple;

SELECT * FROM partitioned_by_tuple ORDER BY d, x, w, y;

OPTIMIZE STREAM partitioned_by_tuple FINAL;

SELECT * FROM partitioned_by_tuple ORDER BY d, x, w, y;

DROP STREAM partitioned_by_tuple;
