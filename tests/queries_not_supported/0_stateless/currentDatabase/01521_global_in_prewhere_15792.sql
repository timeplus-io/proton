-- Tags: global

SET query_mode = 'table';
drop stream if exists xp;
drop stream if exists xp_d;

create stream xp(A date, B int64, S string) Engine=MergeTree partition by to_YYYYMM(A) order by B;
insert into xp select '2020-01-01', number , '' from numbers(100000);

create stream xp_d as xp Engine=Distributed(test_shard_localhost, currentDatabase(), xp);

select count() from xp_d prewhere to_YYYYMM(A) global in (select to_YYYYMM(min(A)) from xp_d) where B > -1;

drop stream if exists xp;
drop stream if exists xp_d;
