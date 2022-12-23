select * from (select number, int_div(number,5) value from numbers(20) order by value limit 3 with ties) ORDER BY number, value;
SET query_mode = 'table';
drop stream if exists wt;
create stream wt (a int, b int) engine = Memory;
insert into wt select 0, number from numbers(5);

select 1 from wt order by a limit 3 with ties;
select b from (select b from wt order by a limit 3 with ties) order by b;
select * from (select * from (with a * 2 as c select a, b from wt order by c limit 3 with ties) order by a, b);
select * from (select * from (select a * 2 as c, b from wt order by c limit 3 with ties) order by c, b);

drop stream if exists wt;
