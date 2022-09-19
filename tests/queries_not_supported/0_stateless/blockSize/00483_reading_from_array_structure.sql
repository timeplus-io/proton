SET query_mode = 'table';
drop stream if exists `table_00483`;
create stream `table_00483` (date date, `Struct.Key1` array(uint64), `Struct.Key2` array(uint64), padding FixedString(16)) engine = MergeTree(date, (date), 16);
insert into `table_00483` select today() as date, [number], [number + 1], to_fixed_string('', 16) from system.numbers limit 100;
set preferred_max_column_in_block_size_bytes = 96;
select blockSize(), * from `table_00483` prewhere `Struct.Key1`[1] = 19 and `Struct.Key2`[1] >= 0 format Null;

drop stream if exists `table_00483`;
create stream `table_00483` (date date, `Struct.Key1` array(uint64), `Struct.Key2` array(uint64), padding FixedString(16), x uint64) engine = MergeTree(date, (date), 8);
insert into `table_00483` select today() as date, [number], [number + 1], to_fixed_string('', 16), number from system.numbers limit 100;
set preferred_max_column_in_block_size_bytes = 112;
select blockSize(), * from `table_00483` prewhere x = 7 format Null;

drop stream if exists `table_00483`;
