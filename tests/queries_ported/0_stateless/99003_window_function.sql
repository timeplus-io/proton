drop stream if exists v;
create stream v(id int);
select sum(id) over (w1) from v window w1 as ();  -- { serverError 48 }
select sum(id) over w1 from v window w1 as (); -- { serverError 48 }
select sum(id) over () from v; -- { serverError 48 }
drop stream if exists v;
