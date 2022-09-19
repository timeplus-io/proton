SET query_mode = 'table';
drop stream if exists t;

create stream t (s uint16, l uint16, projection p (select s, l  order by l)) engine MergeTree order by s;

select s from t join (select to_uint16(1) as s) x using (s) settings allow_experimental_projection_optimization = 1;
select s from t join (select to_uint16(1) as s) x using (s) settings allow_experimental_projection_optimization = 0;

drop stream t;
