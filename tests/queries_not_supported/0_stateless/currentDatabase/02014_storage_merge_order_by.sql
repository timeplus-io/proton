DROP STREAM IF EXISTS short;
DROP STREAM IF EXISTS long;
DROP STREAM IF EXISTS merged;

create stream short (e int64, t datetime ) ENGINE = MergeTree PARTITION BY e ORDER BY t;
create stream long (e int64, t datetime ) ENGINE = MergeTree PARTITION BY (e, to_start_of_month(t)) ORDER BY t;

insert into short select number % 11, to_datetime('2021-01-01 00:00:00') + number from numbers(1000);
insert into long select number % 11, to_datetime('2021-01-01 00:00:00') + number from numbers(1000);

create stream merged as short ENGINE = Merge(currentDatabase(), 'short|long');

select sum(e) from (select * from merged order by t limit 10) SETTINGS optimize_read_in_order = 0;

select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 1;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 3;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 10;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 50;

DROP STREAM IF EXISTS short;
DROP STREAM IF EXISTS long;
DROP STREAM IF EXISTS merged;
