-- Tags: no-parallel
SET query_mode = 'table';
drop stream if exists mt_compact;
drop stream if exists mt_compact_2;

create stream mt_compact (a int, s string) engine = MergeTree order by a partition by a
settings index_granularity_bytes = 0;
alter stream mt_compact modify setting min_rows_for_wide_part = 1000; -- { serverError 48 }
show create stream mt_compact;

create stream mt_compact_2 (a int, s string) engine = MergeTree order by a partition by a
settings min_rows_for_wide_part = 1000;
insert into mt_compact_2 values (1, 'a');
alter stream mt_compact attach partition 1 from mt_compact_2; -- { serverError 36 }

drop stream mt_compact;
drop stream mt_compact_2;

set send_logs_level = 'error';
create stream mt_compact (a int, s string) engine = MergeTree order by a partition by a
settings index_granularity_bytes = 0, min_rows_for_wide_part = 1000;

-- Check that alter of other settings works
alter stream mt_compact modify setting parts_to_delay_insert = 300;
alter stream mt_compact modify setting min_rows_for_wide_part = 0;

show create stream mt_compact;

drop stream mt_compact
