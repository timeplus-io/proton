-- this test checks that null values are correctly serialized inside minmax index (issue #7113)
SET query_mode = 'table';
drop stream if exists null_01016;
create stream if not exists null_01016 (x nullable(string)) engine MergeTree order by if_null(x, 'order-null') partition by if_null(x, 'partition-null');
insert into null_01016 values (null);
drop table null_01016;
