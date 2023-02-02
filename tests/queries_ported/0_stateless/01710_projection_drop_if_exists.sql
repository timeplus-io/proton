drop stream if exists tp;

create stream tp (x int32, y int32, projection p (select x, y order by x)) engine = MergeTree order by y;

alter stream tp drop projection pp; -- { serverError 582 }
alter stream tp drop projection if exists pp;
alter stream tp drop projection if exists p;
alter stream tp drop projection p;  -- { serverError 582 }
alter stream tp drop projection if exists p;

drop stream tp;
