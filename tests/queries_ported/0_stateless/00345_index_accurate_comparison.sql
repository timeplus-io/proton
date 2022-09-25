SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS index;

create stream index
(
    key int32,
    name string,
    merge_date date
) ENGINE = MergeTree(merge_date, key, 8192);

insert into index values (1,'1','2016-07-07');
insert into index values (-1,'-1','2016-07-07');

select * from index where key = 1;
select * from index where key = -1;
OPTIMIZE TABLE index;
select * from index where key = 1;
select * from index where key = -1;
select * from index where key < -0.5;

DROP STREAM index;
