-- this test checks that null values are correctly serialized inside minmax index (issue #7113)
SET query_mode = 'table';
drop stream if exists null_01016;
create stream if not exists null_01016 (x Nullable(string)) engine MergeTree order by ifNull(x, 'order-null') partition by ifNull(x, 'partition-null');
insert into null_01016 values (null);
drop stream null_01016;
