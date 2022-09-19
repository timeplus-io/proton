SET query_mode = 'table';
drop stream if exists numbers_10;

create stream numbers_10 (number uint64) engine = MergeTree order by number;
insert into numbers_10 select number from system.numbers limit 10;

SELECT number, (number, to_date('2015-01-01') + number) FROM numbers_10 LIMIT 10 SETTINGS extremes = 1;

drop stream if exists numbers_10;
