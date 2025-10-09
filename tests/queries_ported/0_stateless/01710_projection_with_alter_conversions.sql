-- Tags: disabled
-- https://github.com/timeplus-io/proton-enterprise/issues/9989

drop stream if exists t;

create stream t (i int, j int, projection p (select i order by i)) engine MergeTree order by i;

insert into t values (1, 2);

system stop merges t;

set alter_sync = 0;

alter stream t rename column j to k;

select * from t;

drop stream t;
