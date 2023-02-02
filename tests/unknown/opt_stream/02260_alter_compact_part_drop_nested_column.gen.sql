

DROP STREAM IF EXISTS compact_alter_100000;
CREATE STREAM compact_alter_100000 (d Date, s string, k uint64) ENGINE=MergeTree() PARTITION BY d ORDER BY k SETTINGS min_bytes_for_wide_part=100000;

INSERT INTO compact_alter_100000 VALUES ('2015-01-01', '2015-01-01 00:00:00', 10);

ALTER STREAM compact_alter_100000 ADD COLUMN n.d int;
-- force columns creation
OPTIMIZE STREAM compact_alter_100000 FINAL;
-- this command will not drop n.d from compact part columns.txt
ALTER STREAM compact_alter_100000 DROP COLUMN n;
-- and now modify column will trigger READ_COLUMN of n.d, and it will bail
ALTER STREAM compact_alter_100000 MODIFY COLUMN s DateTime('UTC') DEFAULT '1970-01-01 00:00:00';

DROP STREAM compact_alter_100000;

DROP STREAM IF EXISTS compact_alter_0;
CREATE STREAM compact_alter_0 (d Date, s string, k uint64) ENGINE=MergeTree() PARTITION BY d ORDER BY k SETTINGS min_bytes_for_wide_part=0;

INSERT INTO compact_alter_0 VALUES ('2015-01-01', '2015-01-01 00:00:00', 10);

ALTER STREAM compact_alter_0 ADD COLUMN n.d int;
-- force columns creation
OPTIMIZE STREAM compact_alter_0 FINAL;
-- this command will not drop n.d from compact part columns.txt
ALTER STREAM compact_alter_0 DROP COLUMN n;
-- and now modify column will trigger READ_COLUMN of n.d, and it will bail
ALTER STREAM compact_alter_0 MODIFY COLUMN s DateTime('UTC') DEFAULT '1970-01-01 00:00:00';

DROP STREAM compact_alter_0;

