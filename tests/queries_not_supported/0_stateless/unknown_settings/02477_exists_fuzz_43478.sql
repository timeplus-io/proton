create stream test_rows_compact_part__fuzz_11 (x uint32) engine = MergeTree order by x;
insert into test_rows_compact_part__fuzz_11 select 1;
select 1 from test_rows_compact_part__fuzz_11 where exists(select 1) settings allow_experimental_analyzer=1;
