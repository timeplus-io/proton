SET query_mode = 'table';
drop stream if exists tp;

create stream tp (type int32, eventcnt uint64, projection p (select sum(eventcnt), type group by type order by sum(eventcnt))) engine = MergeTree order by type; -- { serverError 583 }

drop stream if exists tp;
