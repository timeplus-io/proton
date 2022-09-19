-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug
SET query_mode = 'table';
drop stream if exists tab_lc;
create stream tab_lc (x uint64, y LowCardinality(string)) engine = MergeTree order by x;
insert into tab_lc select number, to_string(number % 10) from numbers(20000000);
optimize table tab_lc;
select count() from tab_lc where y == '0' settings local_filesystem_read_prefetch=1;
drop stream if exists tab_lc;
