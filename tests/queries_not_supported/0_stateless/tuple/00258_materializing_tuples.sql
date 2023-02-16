select * from (select tuple_cast(1) as a union all select tuple_cast(1) as a) order by a;
select * from (select tuple_cast(1) as a union all select tuple_cast(2) as a) order by a;
select * from (select tuple_cast(materialize(0)) as a union all select tuple_cast(0) as a) order by a;
select * from (select tuple_cast(range(1)[1]) as a union all select tuple_cast(0) as a) order by a;
select * from (select tuple_cast(range(1)[2]) as a union all select tuple_cast(1) as a) order by a;
