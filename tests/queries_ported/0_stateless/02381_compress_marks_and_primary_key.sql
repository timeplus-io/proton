-- Tags: no-backward-compatibility-check

drop stream if exists test_02381;
create stream test_02381(a uint64, b uint64) ENGINE = MergeTree order by (a, b) SETTINGS compress_marks=false, compress_primary_key=false;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

drop stream if exists test_02381_compress;
create stream test_02381_compress(a uint64, b uint64) ENGINE = MergeTree order by (a, b)
    SETTINGS compress_marks=true, compress_primary_key=true, marks_compression_codec='ZSTD(3)', primary_key_compression_codec='ZSTD(3)', marks_compress_block_size=65536, primary_key_compress_block_size=65536;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 1000 limit 1;
optimize table test_02381_compress final;
select * from test_02381_compress where a = 1000 limit 1;

-- Compare the size of marks on disk
select table, sum(rows), sum(bytes_on_disk) as sum_bytes, sum(marks_bytes) as sum_marks_bytes, (sum_bytes - sum_marks_bytes) as exclude_marks from system.parts_columns where active and database = current_database() and table like 'test_02381%' group by table order by table;

-- Switch to compressed and uncompressed
-- Test wide part
alter stream test_02381 modify setting compress_marks=true, compress_primary_key=true;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

alter stream test_02381_compress modify setting compress_marks=false, compress_primary_key=false;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 10000 limit 1;
optimize table test_02381_compress final;
select * from test_02381_compress where a = 10000 limit 1;

select * from test_02381 where a = 10000 limit 1;
optimize table test_02381 final;
select * from test_02381 where a = 10000 limit 1;

select table, sum(rows), sum(bytes_on_disk) as sum_bytes, sum(marks_bytes) as sum_marks_bytes, (sum_bytes - sum_marks_bytes) as exclude_marks  from system.parts_columns where active and  database = current_database() and table like 'test_02381%' group by table order by table;

drop stream if exists test_02381;
drop stream if exists test_02381_compress;

-- Test compact part
drop stream if exists test_02381_compact;
create stream test_02381_compact (a uint64, b string) ENGINE = MergeTree order by (a, b);

insert into test_02381_compact values (1, 'Hello');
alter stream test_02381_compact modify setting compress_marks = true, compress_primary_key = true;
insert into test_02381_compact values (2, 'World');

select * from test_02381_compact order by a;
optimize table test_02381_compact final;
select * from test_02381_compact order by a;

drop stream if exists test_02381_compact;
