-- Tags: no-parallel

DROP STREAM IF EXISTS t2;

CREATE STREAM t2(id uint32, a uint64, s string)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

SYSTEM STOP MERGES t2;

INSERT INTO t2 VALUES (1, 2, 'foo'), (1, 3, 'bar');
INSERT INTO t2 VALUES (2, 4, 'aa'), (2, 5, 'bb'), (2, 6, 'cc');
INSERT INTO t2 VALUES (3, 7, 'qq'), (3, 8, 'ww'), (3, 9, 'ee'), (3, 10, 'rr');

SELECT * FROM t2 ORDER BY a;
SELECT name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT '^ init ==================';

ALTER STREAM t2 DROP PARTITION 1;
SELECT * FROM t2 ORDER BY a;
SELECT name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT '^ drop 1 ==================';

ALTER STREAM t2 DETACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT '^ detach 2 ==================';

ALTER STREAM t2 ATTACH PARTITION 2;
SELECT * FROM t2 ORDER BY a;
SELECT name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT '^ attach 2 =================';

DETACH STREAM t2;
ATTACH STREAM t2;

SELECT * FROM t2 ORDER BY a;
SELECT name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT '^ detach attach ==================';

DROP STREAM IF EXISTS t3;

CREATE STREAM t3(id uint32, a uint64, s string)
    ENGINE = MergeTree ORDER BY a PARTITION BY id
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_wide_part = 2000;

INSERT INTO t3 VALUES (3, 11, 'tt'), (3, 12, 'yy');
ALTER STREAM t2 REPLACE PARTITION 3 FROM t3;
SELECT * FROM t2 ORDER BY a;
SELECT table, name, part_type, rows FROM system.parts WHERE table = 't2' AND active AND database = current_database() ORDER BY name;
SELECT table, name, part_type, rows FROM system.parts WHERE table = 't3' AND active AND database = current_database() ORDER BY name;
SELECT '^ replace ==================';

ALTER STREAM t3 FREEZE PARTITION 3;
SELECT name, part_type, is_frozen, rows FROM system.parts WHERE table = 't3' AND active AND database = current_database() ORDER BY name;
SELECT '^ freeze ==================';

DROP STREAM t2;
DROP STREAM t3;
