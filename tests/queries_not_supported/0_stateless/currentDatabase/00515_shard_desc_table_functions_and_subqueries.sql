-- Tags: shard
SET query_mode = 'table';
drop stream if exists tab;
create stream tab (date date, val uint64, val2 uint8 default 42, val3 uint8 default val2 + 1, val4 uint64 alias val) engine = MergeTree(date, (date, val), 8192);
desc tab;
select '-';
desc table tab;
select '-';
desc remote('127.0.0.2', currentDatabase(), tab);
select '-';
desc table remote('127.0.0.2', currentDatabase(), tab);
select '-';
desc (select 1);
select '-';
desc table (select 1);
select '-';
desc (select * from system.numbers);
select '-';
drop stream if exists tab;
