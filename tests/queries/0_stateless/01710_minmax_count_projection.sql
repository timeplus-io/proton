SET query_mode = 'table';
drop stream if exists d;

create stream d (i int, j int) engine MergeTree partition by i % 2 order by tuple() settings index_granularity = 1;

insert into d select number, number from numbers(10000);

set max_rows_to_read = 2, allow_experimental_projection_optimization = 1;

select min(i), max(i), count() from d;
select min(i), max(i), count() from d group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 0 group by _partition_id order by _partition_id;
select min(i), max(i), count() from d where _partition_value.1 = 10 group by _partition_id order by _partition_id;

-- fuzz crash
select min(i) from d where 1 = _partition_value.1;

drop stream d;

drop stream if exists has_final_mark;
drop stream if exists mixed_final_mark;

create stream has_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10, write_final_mark = 1;
create stream mixed_final_mark (i int, j int) engine MergeTree partition by i % 2 order by j settings index_granularity = 10;

set max_rows_to_read = 100000;

insert into has_final_mark select number, number from numbers(10000);

alter stream mixed_final_mark attach partition 1 from has_final_mark;

set max_rows_to_read = 2;

select min(j) from has_final_mark;
select min(j) from mixed_final_mark;

select min(j), max(j) from has_final_mark;

set max_rows_to_read = 5001; -- one normal part 5000 + one minmax_count_projection part 1
select min(j), max(j) from mixed_final_mark;

-- The first primary expr is the same of some partition column
drop stream if exists t;
create stream t (server_date date, something string) engine MergeTree partition by (toYYYYMM(server_date), server_date) order by (server_date, something);
insert into t values ('2019-01-01', 'test1'), ('2019-02-01', 'test2'), ('2019-03-01', 'test3');
select count() from t;
drop stream t;

drop stream if exists d;
create stream d (dt DateTime, j int) engine MergeTree partition by (to_date(dt), ceiling(j), to_date(dt), CEILING(j)) order by tuple();
insert into d values ('2021-10-24 10:00:00', 10), ('2021-10-25 10:00:00', 10), ('2021-10-26 10:00:00', 10), ('2021-10-27 10:00:00', 10);
select min(dt), max(dt), count() from d where to_date(dt) >= '2021-10-25';
select count() from d group by to_date(dt);

-- fuzz crash
SELECT pointInEllipses(min(j), NULL), max(dt), count('0.0000000007') FROM d WHERE to_date(dt) >= '2021-10-25';
SELECT min(dt) FROM d PREWHERE ceil(j) <= 0;
SELECT min(dt) FROM d PREWHERE ((0.9998999834060669 AND 1023) AND 255) <= ceil(j);
SELECT count('') AND NULL FROM d PREWHERE ceil(j) <= NULL;

drop stream d;
