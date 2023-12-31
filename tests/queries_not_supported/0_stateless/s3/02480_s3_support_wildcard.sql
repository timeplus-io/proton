-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS

-- { echo }
drop stream if exists test_02480_write;
drop stream if exists test_02480_write2;
create stream test_02480_write (a uint64, b string) engine = S3(s3_conn, filename='test_02480_{_partition_id}', format=Parquet) partition by a;
set s3_truncate_on_insert=1;
insert into test_02480_write values (1, 'a'), (22, 'b'), (333, 'c');

select a, b from s3(s3_conn, filename='test_02480_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_??', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_?*?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_{1,333}', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='test_02480_{1..333}', format=Parquet) order by a;

create stream test_02480_write2 (a uint64, b string) engine = S3(s3_conn, filename='prefix/test_02480_{_partition_id}', format=Parquet) partition by a;
set s3_truncate_on_insert=1;
insert into test_02480_write2 values (4, 'd'), (55, 'f'), (666, 'g');

select a, b from s3(s3_conn, filename='*/test_02480_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='*/test_02480_?', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='prefix/test_02480_??', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='prefi?/test_02480_*', format=Parquet) order by a;
select a, b from s3(s3_conn, filename='p?*/test_02480_{56..666}', format=Parquet) order by a;

drop stream test_02480_write;
drop stream test_02480_write2;
