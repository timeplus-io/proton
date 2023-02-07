-- Tags: no-fasttest

drop stream if exists test_02312;
create stream test_02312 (x tuple(a uint32, b uint32)) engine=File(Parquet);
insert into test_02312 values ((1,2)), ((2,3)), ((3,4));
select * from test_02312;
drop stream test_02312;
create stream test_02312 (x tuple(a uint32, b uint32)) engine=File(Arrow);
insert into test_02312 values ((1,2)), ((2,3)), ((3,4));
select * from test_02312;
drop stream test_02312;
create stream test_02312 (x tuple(a uint32, b uint32)) engine=File(ORC);
insert into test_02312 values ((1,2)), ((2,3)), ((3,4));
select * from test_02312;
drop stream test_02312;

create stream test_02312 (a Nested(b Nested(c uint32))) engine=File(Parquet);
insert into test_02312 values ([[(1), (2), (3)]]);
select * from test_02312;
drop stream test_02312;
create stream test_02312 (a Nested(b Nested(c uint32))) engine=File(Arrow);
insert into test_02312 values ([[(1), (2), (3)]]);
select * from test_02312;
drop stream test_02312;
create stream test_02312 (a Nested(b Nested(c uint32))) engine=File(ORC);
insert into test_02312 values ([[(1), (2), (3)]]);
select * from test_02312;
drop stream test_02312;

