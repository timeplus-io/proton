SET query_mode = 'table';
drop stream if exists tab;
create stream tab (x uint64) engine = MergeTree order by tuple();

insert into tab select number as n from numbers(20) nums
semi left join (select number * 10 as n from numbers(2)) js2 using(n)
settings max_block_size = 5;
select * from tab order by x;

drop stream tab;
