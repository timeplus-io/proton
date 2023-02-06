-- Tags: no-fasttest

insert into stream function file('/dev/null', 'Parquet', 'number uint64') select * from numbers(10);
insert into stream function file('/dev/null', 'ORC', 'number uint64') select * from numbers(10);
insert into stream function file('/dev/null', 'JSON', 'number uint64') select * from numbers(10);

