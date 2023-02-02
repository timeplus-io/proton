drop stream if exists test1;

create stream test1 (i int64) engine MergeTree order by i;

insert into test1 values (53), (1777), (53284);

select count() from test1 where to_int16(i) = 1777;

drop stream if exists test1;
