set allow_experimental_projection_optimization = 1;
SET query_mode = 'table';

drop stream if exists x;

create stream x (pk int, arr array(int), projection p (select arr order by pk)) engine MergeTree order by tuple();

insert into x values (1, [2]);

select a from x array join arr as a;

drop stream x;
