SET query_mode = 'table';
drop stream if exists t;
create stream t engine = Memory as with cte as (select * from numbers(10)) select * from cte;
drop stream t;

drop stream if exists view1;
create view view1 as with t as (select number n from numbers(3)) select n from t;
drop stream view1;
