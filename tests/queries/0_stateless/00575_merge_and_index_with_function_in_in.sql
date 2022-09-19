DROP STREAM IF EXISTS t_00575;

create stream t_00575(d date) engine MergeTree(d, d, 8192);

insert into t_00575 values ('2018-02-20');

select count() from t_00575 where to_day_of_week(d) in (2);

DROP STREAM t_00575;
