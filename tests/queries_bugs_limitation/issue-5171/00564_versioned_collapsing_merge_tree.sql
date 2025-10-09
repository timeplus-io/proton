-- Tags: no-parallel

set optimize_on_insert = 0;

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


SET query_mode = 'table';
drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date), 8192, sign, version);
insert into mult_tab(date, value, version, sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date, value, version, sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, version, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 4 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 4 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 0, 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 1, 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 2, 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 5 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 5 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date, value), 8192, sign, version);
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 1000000;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 1000000;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value uint64, key uint64, version uint64, sign int8) engine = VersionedCollapsingMergeTree(date, (date), 8192, sign, version);
insert into mult_tab(date,value,key, version,sign) select '2018-01-31', number, number, 0, if(number < 64, 1, -1) from system.numbers limit 128;
insert into mult_tab(date,value,key, version,sign) select '2018-01-31', number, number + 128, 0, if(number < 64, -1, 1) from system.numbers limit 128;
select sleep(3);
select 'table with 2 blocks final';
select date, value, version, sign from mult_tab final order by date, key, sign settings max_block_size=33;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select date, value, version, sign from mult_tab;

select '-------------------------';
select 'Vertival merge';
select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, version, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
select sleep(3);
select 'table with 4 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 4 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 0, 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 1, 1, -1) from system.numbers limit 10;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 1, if(number % 3 = 2, 1, -1) from system.numbers limit 10;
select sleep(3);
select 'table with 5 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 5 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value string, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date, value) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), 1, -1) from system.numbers limit 1000000;
insert into mult_tab(date,value,version,sign) select '2018-01-31', 'str_' || to_string(number), 0, if(to_bool(number %2), -1, 1) from system.numbers limit 1000000;
select sleep(3);
select 'table with 2 blocks final';
select * from mult_tab final order by date, value, sign;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select * from mult_tab;

select '-------------------------';

drop stream if exists mult_tab;
create stream mult_tab (date date, value uint64, key uint64, version uint64, sign int8) engine = VersionedCollapsingMergeTree(sign, version) order by (date) settings enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 0;
insert into mult_tab(date,value,key,version,sign) select '2018-01-31', number, number, 0, if(number < 64, 1, -1) from system.numbers limit 128;
insert into mult_tab(date,value,key,version,sign) select '2018-01-31', number, number + 128, 0, if(number < 64, -1, 1) from system.numbers limit 128;
select sleep(3);
select 'table with 2 blocks final';
select date, value, version, sign from mult_tab final order by date, key, sign settings max_block_size=33;
optimize table mult_tab;
select 'table with 2 blocks optimized';
select date, value, version, sign from mult_tab;

DROP STREAM mult_tab;
