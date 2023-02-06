drop stream if exists tp;

create stream tp (d1 int32, d2 int32, eventcnt int64, projection p (select sum(eventcnt) group by d1)) engine = MergeTree order by (d1, d2);

set allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

select sum(eventcnt) as eventcnt, d1 from tp group by d1;

select avg(eventcnt) as eventcnt, d1 from tp group by d1;

insert into tp values (1, 2, 3);

select sum(eventcnt) as eventcnt, d1 from tp group by d1;

select avg(eventcnt) as eventcnt, d1 from tp group by d1; -- { serverError 584 }

drop stream tp;
