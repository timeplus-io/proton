drop stream if exists bad_date_time;

create stream bad_date_time (time Datetime('Asia/Istanbul'), count uint16) Engine = MergeTree() ORDER BY (time);

insert into bad_date_time values('2020-12-20 20:59:52', 1),  ('2020-12-20 21:59:52', 1),  ('2020-12-20 01:59:52', 1);

-- primary key analysis was wrong in previous versions and did not take the timezone argument into account, so empty result was given.
select to_date(time, 'UTC') as dt, min(to_datetime(time, 'UTC')), max(to_datetime(time, 'UTC')), sum(count) from bad_date_time where to_date(time, 'UTC') = '2020-12-19' group by dt;

drop stream if exists bad_date_time;
