SET query_mode = 'table';
drop stream if exists tab_00712_1;
create stream tab_00712_1 (a uint32, b uint32 alias a + 1, c uint32) engine = MergeTree order by tuple();
insert into tab_00712_1 values (1, 2);
select ignore(_part) from tab_00712_1 prewhere b = 2;
drop stream tab_00712_1;
