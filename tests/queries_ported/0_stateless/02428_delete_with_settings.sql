drop stream if exists test;
create stream test (id int32, key string) engine=MergeTree() order by id;
insert into test select number, to_string(number) from numbers(1000000);
delete from test where id % 2 = 0 SETTINGS mutations_sync=0;
select count() from test;
