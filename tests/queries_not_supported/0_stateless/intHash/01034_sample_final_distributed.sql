-- Tags: distributed
SET query_mode = 'table';
drop stream if exists sample_final;
create stream sample_final (CounterID uint32, EventDate date, EventTime DateTime, UserID uint64, Sign int8) engine = CollapsingMergeTree(Sign) order by (CounterID, EventDate, intHash32(UserID), EventTime) sample by intHash32(UserID);
insert into sample_final select number / (8192 * 4), to_date('2019-01-01'), to_datetime('2019-01-01 00:00:01') + number, number / (8192 * 2), number % 3 = 1 ? -1 : 1 from numbers(1000000);

select 'count';
select count() from sample_final;
select 'count final';
select count() from sample_final final;
select 'count sample';
select count() from sample_final sample 1/2;
select 'count sample final';
select count() from sample_final final sample 1/2;
select 'count final max_parallel_replicas';
set max_parallel_replicas=2;
select count() from remote('127.0.0.{2|3}', currentDatabase(), sample_final) final;

drop stream if exists sample_final;
