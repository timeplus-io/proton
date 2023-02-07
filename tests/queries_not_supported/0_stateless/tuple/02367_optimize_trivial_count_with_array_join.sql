drop stream if exists t;
drop stream if exists t1;

create stream t(id uint32) engine MergeTree order by id as select 1;

create stream t1(a array(uint32)) ENGINE = MergeTree ORDER BY tuple() as select [1,2];

select count() from t array join (select a from t1) AS _a settings optimize_trivial_count_query=1;

drop stream t;
drop stream t1;
