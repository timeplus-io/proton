SET query_mode = 'table';
drop stream if exists tab;
create stream tab (x low_cardinality(string)) engine = MergeTree order by tuple();

insert into tab values ('a'), ('bb'), ('a'), ('cc');

select count() as c, x in ('a', 'bb') as g from tab group by g order by c;

drop stream if exists tab;
