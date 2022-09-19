-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP STREAM IF EXISTS replicated_alter1;
DROP STREAM IF EXISTS replicated_alter2;

SET replication_alter_partitions_sync = 2;

create stream replicated_alter1 (d date, k uint64, i32 int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00062/alter', 'r1', d, k, 8192);
create stream replicated_alter2 (d date, k uint64, i32 int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00062/alter', 'r2', d, k, 8192);

INSERT INTO replicated_alter1 VALUES ('2015-01-01', 10, 42);

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN dt DateTime('UTC');
INSERT INTO replicated_alter1 VALUES ('2015-01-01', 9, 41, '1992-01-01 08:00:00');

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN n nested(ui8 uint8, s string);
INSERT INTO replicated_alter1 VALUES ('2015-01-01', 8, 40, '2012-12-12 12:12:12', [1,2,3], ['12','13','14']);

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN `n.d` array(date);
INSERT INTO replicated_alter1 VALUES ('2015-01-01', 7, 39, '2014-07-14 13:26:50', [10,20,30], ['120','130','140'],['2000-01-01','2000-01-01','2000-01-03']);

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN s string DEFAULT '0';
INSERT INTO replicated_alter1 VALUES ('2015-01-01', 6,38,'2014-07-15 13:26:50',[10,20,30],['asd','qwe','qwe'],['2000-01-01','2000-01-01','2000-01-03'],'100500');

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 DROP COLUMN `n.d`, MODIFY COLUMN s Int64;

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN `n.d` array(date), MODIFY COLUMN s uint32;

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 DROP COLUMN n.ui8, DROP COLUMN n.d;

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 DROP COLUMN n.s;

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 ADD COLUMN n.s array(string), ADD COLUMN n.d array(date);

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 DROP COLUMN n;

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

ALTER STREAM replicated_alter1 MODIFY COLUMN dt date, MODIFY COLUMN s DateTime('UTC') DEFAULT '1970-01-01 00:00:00';

DESC STREAM replicated_alter1;
SHOW create stream replicated_alter1;
DESC STREAM replicated_alter2;
SHOW create stream replicated_alter2;
SELECT * FROM replicated_alter1 ORDER BY k;

DROP STREAM replicated_alter1;
DROP STREAM replicated_alter2;
