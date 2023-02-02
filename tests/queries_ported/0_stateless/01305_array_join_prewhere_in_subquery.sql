drop stream if exists h;
create stream h (EventDate Date, CounterID uint64, WatchID uint64) engine = MergeTree order by (CounterID, EventDate);
insert into h values ('2020-06-10', 16671268, 1);
SELECT count() from h ARRAY JOIN [1] AS a PREWHERE WatchID IN (SELECT to_uint64(1)) WHERE (EventDate = '2020-06-10') AND (CounterID = 16671268);
drop stream if exists h;
