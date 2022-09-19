-- Tags: distributed
SET query_mode = 'table';
create stream if not exists sample_prewhere (date date, id int32, time Int64) engine = MergeTree partition by date order by (id, time, intHash64(time)) sample by intHash64(time);

insert into sample_prewhere values ('2019-01-01', 2, to_datetime('2019-07-20 00:00:01'));
insert into sample_prewhere values ('2019-01-01', 1, to_datetime('2019-07-20 00:00:02'));
insert into sample_prewhere values ('2019-01-02', 3, to_datetime('2019-07-20 00:00:03'));

select id from remote('127.0.0.{1,3}', currentDatabase(), sample_prewhere) SAMPLE 1 where to_datetime(time) = '2019-07-20 00:00:00';

drop stream sample_prewhere;
