-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop stream if exists test_02245;
create stream test_02245 (a uint64) engine = S3(s3_conn, filename='test_02245', format=Parquet);
insert into test_02245 select 1 settings s3_truncate_on_insert=1;
select * from test_02245;
select _path from test_02245;

drop stream if exists test_02245_2;
create stream test_02245_2 (a uint64, _path int32) engine = S3(s3_conn, filename='test_02245_2', format=Parquet);
insert into test_02245_2 select 1, 2 settings s3_truncate_on_insert=1;
select * from test_02245_2;
select _path from test_02245_2;
