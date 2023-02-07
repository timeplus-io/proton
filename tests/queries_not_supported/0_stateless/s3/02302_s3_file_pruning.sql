-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop stream if exists test_02302;
create stream test_02302 (a uint64) engine = S3(s3_conn, filename='test_02302_{_partition_id}', format=Parquet) partition by a;
insert into test_02302 select number from numbers(10) settings s3_truncate_on_insert=1;
select * from test_02302;  -- { serverError 48 }
drop stream test_02302;

set max_rows_to_read = 1;

-- Test s3 stream function with glob
select * from s3(s3_conn, filename='test_02302_*', format=Parquet) where _file like '%5';

-- Test s3 stream with explicit keys (no glob)
-- TODO support truncate stream function
drop stream if exists test_02302;
create stream test_02302 (a uint64) engine = S3(s3_conn, filename='test_02302.2', format=Parquet);
truncate stream test_02302;

drop stream if exists test_02302;
create stream test_02302 (a uint64) engine = S3(s3_conn, filename='test_02302.1', format=Parquet);
truncate stream test_02302;

drop stream if exists test_02302;
create stream test_02302 (a uint64) engine = S3(s3_conn, filename='test_02302', format=Parquet);
truncate stream test_02302;

insert into test_02302 select 0 settings s3_create_new_file_on_insert = true;
insert into test_02302 select 1 settings s3_create_new_file_on_insert = true;
insert into test_02302 select 2 settings s3_create_new_file_on_insert = true;

select * from test_02302 where _file like '%1';
drop stream test_02302;
