-- Tags: no-fasttest, no-parallel
insert into stream function file('data.avro', 'Parquet', 'x uint64') select * from numbers(10);
insert into stream function file('data.avro', 'Parquet', 'x uint64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
insert into stream function file('data.avro', 'Parquet', 'x uint64') select * from numbers(10); -- { serverError CANNOT_APPEND_TO_FILE }
select 'OK';
