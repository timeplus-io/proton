DROP STREAM IF EXISTS merge_table_standard_delete;

CREATE STREAM merge_table_standard_delete(id int32, name string) ENGINE = MergeTree order by id settings min_bytes_for_wide_part=10000000;

INSERT INTO merge_table_standard_delete select number, to_string(number) from numbers(100);

SELECT count(), part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'merge_table_standard_delete' AND active GROUP BY part_type ORDER BY part_type;

SET mutations_sync = 0;
SET allow_experimental_lightweight_delete = 1;

DELETE FROM merge_table_standard_delete WHERE id = 10;
SELECT count(), part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'merge_table_standard_delete' AND active GROUP BY part_type ORDER BY part_type;

SELECT count() FROM merge_table_standard_delete;

DETACH STREAM merge_table_standard_delete;
ATTACH STREAM merge_table_standard_delete;
CHECK STREAM merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE name IN ('1','2','3','4');
SELECT count(), part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'merge_table_standard_delete' AND active GROUP BY part_type ORDER BY part_type;

SELECT count() FROM merge_table_standard_delete;

DETACH STREAM merge_table_standard_delete;
ATTACH STREAM merge_table_standard_delete;
CHECK STREAM merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE 1;
SELECT count(), part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'merge_table_standard_delete' AND active GROUP BY part_type ORDER BY part_type;

SELECT count() FROM merge_table_standard_delete;

DETACH STREAM merge_table_standard_delete;
ATTACH STREAM merge_table_standard_delete;
CHECK STREAM merge_table_standard_delete;

DROP STREAM merge_table_standard_delete;

drop stream if exists t_light;
create stream t_light(a int, b int, c int, index i_c(b) type minmax granularity 4) engine = MergeTree order by a partition by c % 5 settings min_bytes_for_wide_part=10000000;
INSERT INTO t_light SELECT number, number, number FROM numbers(10);
SELECT count(), part_type FROM system.parts WHERE database = currentDatabase() AND stream = 't_light' AND active GROUP BY part_type ORDER BY part_type;

SELECT '-----lightweight mutation type-----';

DELETE FROM t_light WHERE c%5=1;

DETACH STREAM t_light;
ATTACH STREAM t_light;
CHECK STREAM t_light;

DELETE FROM t_light WHERE c=4;

DETACH STREAM t_light;
ATTACH STREAM t_light;
CHECK STREAM t_light;

alter stream t_light MATERIALIZE INDEX i_c SETTINGS mutations_sync=2;
alter stream t_light update b=-1 where a<3 SETTINGS mutations_sync=2;
alter stream t_light drop index i_c SETTINGS mutations_sync=2;

DETACH STREAM t_light;
ATTACH STREAM t_light;
CHECK STREAM t_light;

SELECT command, is_done FROM system.mutations WHERE database = currentDatabase() AND stream = 't_light';

SELECT '-----Check that select and merge with lightweight delete.-----';
select count(*) from t_light;
select * from t_light order by a;

select stream, partition, name, rows from system.parts where database = currentDatabase() AND active and stream ='t_light' order by name;

optimize stream t_light final SETTINGS mutations_sync=2;
select count(*) from t_light;

select stream, partition, name, rows from system.parts where database = currentDatabase() AND active and stream ='t_light' and rows > 0 order by name;

drop stream t_light;

SELECT '-----Test lightweight delete in multi blocks-----';
CREATE STREAM t_large(a uint32, b int) ENGINE=MergeTree order BY a settings min_bytes_for_wide_part=0;
INSERT INTO t_large SELECT number + 1, number + 1  FROM numbers(100000);

DELETE FROM t_large WHERE a = 50000;

DETACH STREAM t_large;
ATTACH STREAM t_large;
CHECK STREAM t_large;

ALTER STREAM t_large UPDATE b = -2 WHERE a between 1000 and 1005 SETTINGS mutations_sync=2;
ALTER STREAM t_large DELETE WHERE a=1 SETTINGS mutations_sync=2;

DETACH STREAM t_large;
ATTACH STREAM t_large;
CHECK STREAM t_large;

SELECT * FROM t_large WHERE a in (1,1000,1005,50000) order by a;

DROP STREAM  t_large;
