DROP VIEW IF EXISTS 99114_read_mv_v1;
DROP VIEW IF EXISTS 99114_write_mv1_v1;
DROP VIEW IF EXISTS 99114_write_mv2_v1;
DROP VIEW IF EXISTS 99114_write_mv3_v1;

DROP VIEW IF EXISTS 99114_read_mv_v2;
DROP VIEW IF EXISTS 99114_write_mv1_v2;
DROP VIEW IF EXISTS 99114_write_mv2_v2;
DROP VIEW IF EXISTS 99114_write_mv3_v2;
DROP VIEW IF EXISTS 99114_write_mv4_v2;
DROP VIEW IF EXISTS 99114_write_mv5_v2;
DROP VIEW IF EXISTS 99114_write_mv6_v2;
DROP VIEW IF EXISTS 99114_write_mv7_v2;

DROP VIEW IF EXISTS 99114_read_mv_v3;

DROP STREAM IF EXISTS 99114_stream;

DROP STREAM IF EXISTS 99114_data_gen;

CREATE STREAM 99114_data_gen(
    str_col string,
    int_col int,
    _tp_time datetime64(3, 'UTC') DEFAULT '2025-12-01 00:00:00' CODEC(DoubleDelta, LZ4)
) SETTINGS flush_threshold_count=1;

CREATE STREAM 99114_stream(
    str_col string,
    _tp_time datetime64(3, 'UTC') DEFAULT '2025-12-01 00:00:00' CODEC(DoubleDelta, LZ4)
) SETTINGS flush_threshold_count=1;

SET seek_to='earliest', enable_backfill_from_historical_store=0;

--- Create MatView to read version-1
CREATE MATERIALIZED VIEW 99114_read_mv_v1 AS
    --- 1) select zero physical column
    select _tp_schema_version as version, '' as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 2) select one physical column
    union
    select _tp_schema_version as version, str_col as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(_tp_time) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 3) select two physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
STORAGE_SETTINGS flush_threshold_count=1;

--- Create MatView to write version-1
CREATE MATERIALIZED VIEW 99114_write_mv1_v1 INTO 99114_stream AS select str_col from 99114_data_gen;
CREATE MATERIALIZED VIEW 99114_write_mv2_v1 INTO 99114_stream AS select _tp_time from 99114_data_gen;

CREATE MATERIALIZED VIEW 99114_write_mv3_v1 INTO 99114_stream AS select str_col, _tp_time from 99114_data_gen;

--- Add a column `int_col`, bump to version-2
select sleep(1) format Null;
ALTER STREAM 99114_stream ADD COLUMN int_col int;
select sleep(1) format Null;

--- Create MatView to read version-2
CREATE MATERIALIZED VIEW 99114_read_mv_v2 AS
    --- 1) select zero physical column
    select _tp_schema_version as version, '' as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 2) select one physical column
    union
    select _tp_schema_version as version, str_col as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(_tp_time) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 3) select two physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, str_col as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 4) select three physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, to_string(_tp_time) as val_3, '' as val_4 from 99114_stream
STORAGE_SETTINGS flush_threshold_count=1;

--- Create MatView to write version-2
--- Insert partial data:
CREATE MATERIALIZED VIEW 99114_write_mv1_v2 INTO 99114_stream AS select str_col from 99114_data_gen;
CREATE MATERIALIZED VIEW 99114_write_mv2_v2 INTO 99114_stream AS select int_col from 99114_data_gen;
CREATE MATERIALIZED VIEW 99114_write_mv3_v2 INTO 99114_stream AS select _tp_time from 99114_data_gen;

CREATE MATERIALIZED VIEW 99114_write_mv4_v2 INTO 99114_stream AS select str_col, int_col from 99114_data_gen;
CREATE MATERIALIZED VIEW 99114_write_mv5_v2 INTO 99114_stream AS select str_col, _tp_time from 99114_data_gen;
CREATE MATERIALIZED VIEW 99114_write_mv6_v2 INTO 99114_stream AS select int_col, _tp_time from 99114_data_gen;
--- Insert full data
CREATE MATERIALIZED VIEW 99114_write_mv7_v2 INTO 99114_stream AS select str_col, int_col, _tp_time from 99114_data_gen;

--- Add a column `nf_col`, bump to version-3
select sleep(1) format Null;
ALTER STREAM 99114_stream ADD COLUMN nf_col nullable(float);
select sleep(1) format Null;

--- Create MatView to read version-3
CREATE MATERIALIZED VIEW 99114_read_mv_v3 AS
    --- 1) select zero physical column
    select _tp_schema_version as version, '' as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 2) select one physical column
    union
    select _tp_schema_version as version, str_col as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(nf_col) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(_tp_time) as val_1, '' as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 3) select two physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, str_col as val_1, to_string(nf_col) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, str_col as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, to_string(nf_col) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(nf_col) as val_1, to_string(_tp_time) as val_2, '' as val_3, '' as val_4 from 99114_stream
    --- 4) select three physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, to_string(nf_col) as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, to_string(_tp_time) as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, str_col as val_1, to_string(_tp_time) as val_2, to_string(nf_col) as val_3, '' as val_4 from 99114_stream
    union
    select _tp_schema_version as version, to_string(int_col) as val_1, to_string(nf_col) as val_2, to_string(_tp_time) as val_3, '' as val_4 from 99114_stream
    --- 5) select four physical columns
    union
    select _tp_schema_version as version, str_col as val_1, to_string(int_col) as val_2, to_string(nf_col) as val_3, to_string(_tp_time) as val_4 from 99114_stream
STORAGE_SETTINGS flush_threshold_count=1;

select sleep(3) format Null;

insert into 99114_data_gen(str_col, int_col, _tp_time) values('str_01', 1001, '2025-12-01 10:00:00') ('str_02', 1002, '2025-12-01 11:00:00') ('str_03', 1003, '2025-12-01 12:00:00');

--- Write with latest version-3
insert into 99114_stream(str_col) values('str_04');
insert into 99114_stream(int_col) values(1004);
insert into 99114_stream(nf_col) values(10.5);
insert into 99114_stream(_tp_time) values('2025-12-01 13:00:00');

insert into 99114_stream(str_col, int_col) values('str_05', 1005);
insert into 99114_stream(str_col, nf_col) values('str_06', 20.5);
insert into 99114_stream(str_col, _tp_time) values('str_07', '2025-12-01 14:00:00');
insert into 99114_stream(int_col, nf_col) values(1006, 30.5);
insert into 99114_stream(int_col, _tp_time) values(1007, '2025-12-01 15:00:00');
insert into 99114_stream(nf_col, _tp_time) values(40.5, '2025-12-01 16:00:00');

insert into 99114_stream(str_col, int_col, nf_col) values('str_08', 1008, 50.5);
insert into 99114_stream(str_col, int_col, _tp_time) values('str_09', 1009, '2025-12-01 17:00:00');
insert into 99114_stream(str_col, nf_col, _tp_time) values('str_10', 60.5, '2025-12-01 18:00:00');
insert into 99114_stream(int_col, nf_col, _tp_time) values(1010, 70.5, '2025-12-01 19:00:00');

insert into 99114_stream(str_col, int_col, nf_col, _tp_time) values('str_11', 1011, 80.5, '2025-12-01 20:00:00');
select sleep(3) format Null;

select '===== Read with schema version-1 =====';
SELECT version, val_1, val_2, val_3, val_4 FROM table(99114_read_mv_v1) ORDER BY version, val_1, val_2, val_3, val_4;
select '===== Read with schema version-2 =====';
SELECT version, val_1, val_2, val_3, val_4 FROM table(99114_read_mv_v2) ORDER BY version, val_1, val_2, val_3, val_4;
select '===== Read with schema version-3 =====';
SELECT version, val_1, val_2, val_3, val_4 FROM table(99114_read_mv_v3) ORDER BY version, val_1, val_2, val_3, val_4;
select '===== Source Stream =====';
SELECT str_col, int_col, nf_col, _tp_time FROM table(99114_stream) ORDER BY str_col, int_col, nf_col, _tp_time;

DROP VIEW IF EXISTS 99114_read_mv_v1;
DROP VIEW IF EXISTS 99114_write_mv1_v1;
DROP VIEW IF EXISTS 99114_write_mv2_v1;
DROP VIEW IF EXISTS 99114_write_mv3_v1;

DROP VIEW IF EXISTS 99114_read_mv_v2;
DROP VIEW IF EXISTS 99114_write_mv1_v2;
DROP VIEW IF EXISTS 99114_write_mv2_v2;
DROP VIEW IF EXISTS 99114_write_mv3_v2;
DROP VIEW IF EXISTS 99114_write_mv4_v2;
DROP VIEW IF EXISTS 99114_write_mv5_v2;
DROP VIEW IF EXISTS 99114_write_mv6_v2;
DROP VIEW IF EXISTS 99114_write_mv7_v2;

DROP VIEW IF EXISTS 99114_read_mv_v3;

DROP STREAM IF EXISTS 99114_stream;

DROP STREAM IF EXISTS 99114_data_gen;
