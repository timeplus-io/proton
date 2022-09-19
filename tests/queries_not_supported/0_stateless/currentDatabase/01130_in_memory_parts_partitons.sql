-- Tags: no-parallel

DROP STREAM IF EXISTS t2;

create stream t2(id uint32, a uint64, s string)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

SYSTEM STOP MERGES t2;

INSERT INTO t2 VALUES (1, 2, 'foo'), (1, 3, 'bar');
INSERT INTO t2 VALUES (2, 4, 'aa'), (2, 5, 'bb');
INSERT INTO t2 VALUES (3, 6, 'qq'), (3, 7, 'ww');

SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER STREAM t2 DROP PARTITION 1;
SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER STREAM t2 DETACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT '==================';

ALTER STREAM t2 ATTACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT name, part_type FROM system.parts WHERE table = 't2' AND active AND database = currentDatabase() ORDER BY name;
SELECT '==================';

DETACH TABLE t2;
ATTACH TABLE t2;

SELECT * FROM t2 ORDER BY a;
SELECT '==================';

DROP STREAM IF EXISTS t3;

create stream t3(id uint32, a uint64, s string)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

INSERT INTO t3 VALUES (3, 6, 'cc'), (3, 7, 'dd');
ALTER STREAM t2 REPLACE PARTITION 3 FROM t3;
SELECT * FROM t2 ORDER BY a;
SELECT table, name, part_type FROM system.parts WHERE table = 't2' AND active AND database = currentDatabase() ORDER BY name;
SELECT table, name, part_type FROM system.parts WHERE table = 't3' AND active AND database = currentDatabase() ORDER BY name;
SELECT '==================';

ALTER STREAM t3 FREEZE PARTITION 3;
SELECT name, part_type, is_frozen FROM system.parts WHERE table = 't3' AND active AND database = currentDatabase() ORDER BY name;

DROP STREAM t2;
DROP STREAM t3;
