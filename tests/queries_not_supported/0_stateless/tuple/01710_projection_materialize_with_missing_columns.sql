SET query_mode = 'table';
drop stream if exists x;

create stream x (i int) engine MergeTree order by tuple();
insert into x values (1);
alter stream x add column j int;
alter stream x add projection p_agg (select sum(j));
alter stream x materialize projection p_agg settings mutations_sync = 1;

drop stream x;
