drop stream if exists tab;
create stream tab (x uint64) engine = MergeTree order by x;

insert into tab select n from (SELECT number AS n FROM numbers(20)) as nums
semi left join (select number * 10 as n from numbers(2)) as js2 using(n)
settings max_block_size = 5;
select * from tab order by x;

drop stream tab;