SET query_mode = 'table';

DROP STREAM IF EXISTS alter_00061;
create stream alter_00061 (d date, k uint64, i32 int32) ENGINE=MergeTree(d, k, 8192);

INSERT INTO alter_00061 (d, k i32) VALUES ('2015-01-01', 10, 42);
SELECT sleep(3);
DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 ADD COLUMN n nested(ui8 uint8, s string);
INSERT INTO alter_00061(d, k i32, n) VALUES ('2015-01-01', 8, 40, [1,2,3], ['12','13','14']);
SELECT sleep(3);
DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 ADD COLUMN `n.d` array(date);
INSERT INTO alter_00061 (d, k i32, n, `n.d`) VALUES ('2015-01-01', 7, 39, [10,20,30], ['120','130','140'],['2000-01-01','2000-01-01','2000-01-03']);
SELECT sleep(3);
DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 ADD COLUMN s string DEFAULT '0';
INSERT INTO alter_00061 (d, k i32, n, `n.d`,s) VALUES ('2015-01-01', 6,38,[10,20,30],['asd','qwe','qwe'],['2000-01-01','2000-01-01','2000-01-03'],'100500');
SELECT sleep(3);
DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 DROP COLUMN `n.d`, MODIFY COLUMN s Int64;

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 ADD COLUMN `n.d` array(date), MODIFY COLUMN s uint32;

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

OPTIMIZE STREAM alter_00061;

SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 DROP COLUMN n.ui8, DROP COLUMN n.d;

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 DROP COLUMN n.s;

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 ADD COLUMN n.s array(string), ADD COLUMN n.d array(date);

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER STREAM alter_00061 DROP COLUMN n;

DESC STREAM alter_00061;
SHOW create stream alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

DROP STREAM alter_00061;
