SET query_mode = 'table';
drop stream if exists test_enum;
create stream test_enum (c Nullable(Enum16('A' = 1, 'B' = 2))) engine Log;
insert into test_enum values (1), (NULL);
select * from test_enum;
select to_string(c) from test_enum;
select to_string('aaaa', NULL);
drop stream test_enum;
