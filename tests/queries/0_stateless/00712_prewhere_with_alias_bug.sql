SET query_mode = 'table';
drop stream if exists prewhere_alias;
create stream prewhere_alias (a int32, b int32, c alias a + b) engine = MergeTree order by b;
insert into prewhere_alias values(1, 1);
select a, c + to_int32(1), (c + to_int32(1)) * 2 from prewhere_alias prewhere (c + to_int32(1)) * 2 = 6;
drop stream prewhere_alias;
