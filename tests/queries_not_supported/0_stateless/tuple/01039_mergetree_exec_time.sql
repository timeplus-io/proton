DROP STREAM IF EXISTS tab;
create stream tab (A int64) Engine=MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = 0;
insert into tab select cityHash64(number) from numbers(1000);
select sum(sleep(0.1)) from tab settings max_block_size = 1, max_execution_time=1; -- { serverError 159 }
DROP STREAM IF EXISTS tab;
