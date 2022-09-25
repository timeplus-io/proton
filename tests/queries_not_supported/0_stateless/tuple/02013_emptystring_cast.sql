SET query_mode = 'table';
drop stream if exists test_uint64;
create stream test_uint64 (`data` uint64 Default 0) engine = MergeTree order by tuple();
insert into test_uint64 values ('0'), (NULL), (1), ('2');
drop stream if exists test_uint64;

drop stream if exists test_float64;
create stream test_float64 (`data` float64 Default 0.0) engine = MergeTree order by tuple();
insert into test_float64 values ('0.1'), (NULL), (1.1), ('2.2');
drop stream if exists test_float64;

drop stream if exists test_date;
create stream test_date (`data` date) engine = MergeTree order by tuple();
insert into test_date values ('2021-01-01'), (NULL), ('2021-02-01'), ('2021-03-01');
drop stream if exists test_date;

drop stream if exists test_datetime;
create stream test_datetime (`data` datetime) engine = MergeTree order by tuple();
insert into test_datetime values ('2021-01-01 00:00:00'), (NULL), ('2021-02-01 01:00:00'), ('2021-03-01 02:00:00');
drop stream if exists test_datetime;
