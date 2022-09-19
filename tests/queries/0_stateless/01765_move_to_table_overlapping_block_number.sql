DROP STREAM IF EXISTS t_src;
DROP STREAM IF EXISTS t_dst;

create stream t_src (id uint32, v uint32) ENGINE = MergeTree ORDER BY id PARTITION BY id;
create stream t_dst (id uint32, v uint32) ENGINE = MergeTree ORDER BY id PARTITION BY id;

SYSTEM STOP MERGES t_src;
SYSTEM STOP MERGES t_dst;

INSERT INTO t_dst VALUES (1, 1);
INSERT INTO t_dst VALUES (1, 2);
INSERT INTO t_dst VALUES (1, 3);

INSERT INTO t_src VALUES (1, 4);

ALTER STREAM t_src MOVE PARTITION 1 TO TABLE t_dst;
SELECT *, _part FROM t_dst ORDER BY v;

DROP STREAM t_src;
DROP STREAM t_dst;
