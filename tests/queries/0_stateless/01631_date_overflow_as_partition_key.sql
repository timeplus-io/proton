SET query_mode = 'table';
drop stream if exists dt_overflow;

create stream dt_overflow(d date, i int) engine MergeTree partition by d order by i;

insert into dt_overflow values('2106-11-11', 1);

insert into dt_overflow values('2106-11-12', 1);

select * from dt_overflow ORDER BY d;

drop stream if exists dt_overflow;
