-- Tags: no-parallel

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS mt_with_pk;

create stream mt_with_pk (
  d date DEFAULT '2000-01-01',
  x DateTime,
  y array(uint64),
  z uint64,
  n nested (Age uint8, Name string),
  w Int16 DEFAULT 10
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d) ORDER BY (x, z) SETTINGS index_granularity_bytes=10000; -- write_final_mark=1 by default

SELECT '===test insert===';

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 12:57:57'), [1, 1, 1], 11, [77], ['Joe']), (to_date('2018-10-01'), to_datetime('2018-10-01 16:57:57'), [2, 2, 2], 12, [88], ['Mark']), (to_date('2018-10-01'), to_datetime('2018-10-01 19:57:57'), [3, 3, 3], 13, [99], ['Robert']);

SELECT COUNT(*) FROM mt_with_pk WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1 AND database = currentDatabase();

SELECT '===test merge===';
INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 07:57:57'), [4, 4, 4], 14, [111, 222], ['Lui', 'Dave']), (to_date('2018-10-01'), to_datetime('2018-10-01 08:57:57'), [5, 5, 5], 15, [333, 444], ['John', 'Mike']), (to_date('2018-10-01'), to_datetime('2018-10-01 09:57:57'), [6, 6, 6], 16, [555, 666, 777], ['Alex', 'Jim', 'Tom']);

OPTIMIZE STREAM mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1 AND database = currentDatabase();

SELECT '===test alter===';
ALTER STREAM mt_with_pk MODIFY COLUMN y array(string);

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 05:57:57'), ['a', 'a', 'a'], 14, [888, 999], ['Jack', 'Elvis']);

OPTIMIZE STREAM mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1 AND database = currentDatabase();

SELECT '===test mutation===';
ALTER STREAM mt_with_pk UPDATE w = 0 WHERE 1 SETTINGS mutations_sync = 2;
ALTER STREAM mt_with_pk UPDATE y = ['q', 'q', 'q'] WHERE 1 SETTINGS mutations_sync = 2;

SELECT sum(w) FROM mt_with_pk;
SELECT distinct(y) FROM mt_with_pk;

OPTIMIZE STREAM mt_with_pk FINAL;

SELECT '===test skip_idx===';
ALTER STREAM mt_with_pk ADD INDEX idx1 z + w TYPE minmax GRANULARITY 1;

INSERT INTO mt_with_pk (d, x, y, z, `n.Age`, `n.Name`, w) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 03:57:57'), ['z', 'z', 'z'], 15, [1111, 2222], ['Garry', 'Ron'], 1);

OPTIMIZE STREAM mt_with_pk FINAL;

SELECT COUNT(*) FROM mt_with_pk WHERE z + w > 5000;

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1 AND database = currentDatabase();

DROP STREAM IF EXISTS mt_with_pk;

SELECT '===test alter attach===';
DROP STREAM IF EXISTS alter_attach;

create stream alter_attach (x uint64, p uint8) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p  SETTINGS index_granularity_bytes=10000, write_final_mark=1;

INSERT INTO alter_attach VALUES (1, 1), (2, 1), (3, 1);

ALTER STREAM alter_attach DETACH PARTITION 1;

ALTER STREAM alter_attach ADD COLUMN s string;
INSERT INTO alter_attach VALUES (4, 2, 'Hello'), (5, 2, 'World');

ALTER STREAM alter_attach ATTACH PARTITION 1;
SELECT * FROM alter_attach ORDER BY x;

ALTER STREAM alter_attach DETACH PARTITION 2;
ALTER STREAM alter_attach DROP COLUMN s;
INSERT INTO alter_attach VALUES (6, 3), (7, 3);

ALTER STREAM alter_attach ATTACH PARTITION 2;
SELECT * FROM alter_attach ORDER BY x;

DROP STREAM IF EXISTS alter_attach;
DROP STREAM IF EXISTS mt_with_pk;

SELECT '===test alter update===';
DROP STREAM IF EXISTS alter_update_00806;

create stream alter_update_00806 (d date, e Enum8('foo'=1, 'bar'=2)) Engine = MergeTree PARTITION BY d ORDER BY (d) SETTINGS index_granularity_bytes=10000, write_final_mark=1;

INSERT INTO alter_update_00806 (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO alter_update_00806 (d, e) VALUES ('2018-01-02', 'bar');

ALTER STREAM alter_update_00806 UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02' SETTINGS mutations_sync = 2;

SELECT e FROM alter_update_00806 ORDER BY d;

DROP STREAM IF EXISTS alter_update_00806;

SELECT '===test no pk===';

DROP STREAM IF EXISTS mt_without_pk;

create stream mt_without_pk (
  d date DEFAULT '2000-01-01',
  x DateTime,
  y array(uint64),
  z uint64,
  n nested (Age uint8, Name string),
  w Int16 DEFAULT 10
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d) ORDER BY tuple() SETTINGS index_granularity_bytes=10000, write_final_mark=1;

INSERT INTO mt_without_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 12:57:57'), [1, 1, 1], 11, [77], ['Joe']), (to_date('2018-10-01'), to_datetime('2018-10-01 16:57:57'), [2, 2, 2], 12, [88], ['Mark']), (to_date('2018-10-01'), to_datetime('2018-10-01 19:57:57'), [3, 3, 3], 13, [99], ['Robert']);

SELECT COUNT(*) FROM mt_without_pk WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_without_pk' AND active=1 AND database = currentDatabase();

INSERT INTO mt_without_pk (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 07:57:57'), [4, 4, 4], 14, [111, 222], ['Lui', 'Dave']), (to_date('2018-10-01'), to_datetime('2018-10-01 08:57:57'), [5, 5, 5], 15, [333, 444], ['John', 'Mike']), (to_date('2018-10-01'), to_datetime('2018-10-01 09:57:57'), [6, 6, 6], 16, [555, 666, 777], ['Alex', 'Jim', 'Tom']);

OPTIMIZE STREAM mt_without_pk FINAL;

SELECT COUNT(*) FROM mt_without_pk WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_without_pk' AND active=1 AND database = currentDatabase();

DROP STREAM IF EXISTS mt_without_pk;

SELECT '===test a lot of marks===';

DROP STREAM IF EXISTS mt_with_small_granularity;

create stream mt_with_small_granularity (
  d date DEFAULT '2000-01-01',
  x DateTime,
  y array(uint64),
  z uint64,
  n nested (Age uint8, Name string),
  w Int16 DEFAULT 10
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d) ORDER BY (x, z) SETTINGS index_granularity_bytes=30, min_index_granularity_bytes=20, write_final_mark=1;

INSERT INTO mt_with_small_granularity (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 12:57:57'), [1, 1, 1], 11, [77], ['Joe']), (to_date('2018-10-01'), to_datetime('2018-10-01 16:57:57'), [2, 2, 2], 12, [88], ['Mark']), (to_date('2018-10-01'), to_datetime('2018-10-01 19:57:57'), [3, 3, 3], 13, [99], ['Robert']);

SELECT COUNT(*) FROM mt_with_small_granularity WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_small_granularity' AND active=1 AND database = currentDatabase();

INSERT INTO mt_with_small_granularity (d, x, y, z, `n.Age`, `n.Name`) VALUES (to_date('2018-10-01'), to_datetime('2018-10-01 07:57:57'), [4, 4, 4], 14, [111, 222], ['Lui', 'Dave']), (to_date('2018-10-01'), to_datetime('2018-10-01 08:57:57'), [5, 5, 5], 15, [333, 444], ['John', 'Mike']), (to_date('2018-10-01'), to_datetime('2018-10-01 09:57:57'), [6, 6, 6], 16, [555, 666, 777], ['Alex', 'Jim', 'Tom']);

OPTIMIZE STREAM mt_with_small_granularity FINAL;

SELECT COUNT(*) FROM mt_with_small_granularity WHERE x > to_datetime('2018-10-01 23:57:57');

SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_small_granularity' AND active=1 AND database = currentDatabase();

DROP STREAM IF EXISTS mt_with_small_granularity;
