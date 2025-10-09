drop stream if exists 99015_v;
create stream 99015_v(id int);

select sum(id) over (w1) from 99015_v window w1 as ();  -- { serverError 48 }
select sum(id) over (w1) from table(99015_v) window w1 as ();

select sum(id) over w1 from 99015_v window w1 as (); -- { serverError 48 }
select sum(id) over w1 from table(99015_v) window w1 as ();

select sum(id) over () from 99015_v; -- { serverError 48 }
select sum(id) over () from table(99015_v);

select sum(id) over (partition by id) from 99015_v; -- { serverError 48 }
select sum(id) over (partition by id) from table(99015_v);

drop stream if exists 99015_v;
