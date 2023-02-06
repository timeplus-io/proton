drop stream if exists ttl_test_02129;

create stream ttl_test_02129(a int64, b string, d Date)
Engine=MergeTree partition by d order by a
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

insert into ttl_test_02129 select number, '', '2021-01-01' from numbers(10);
alter stream ttl_test_02129 add column c int64 settings mutations_sync=2;

insert into ttl_test_02129 select number, '', '2021-01-01', 0 from numbers(10);
alter stream  ttl_test_02129 modify TTL (d + INTERVAL 1 MONTH) DELETE WHERE c=1 settings mutations_sync=2;

select * from ttl_test_02129 order by a, b, d, c;
drop stream ttl_test_02129;

drop stream if exists ttl_test_02129;

select '==========';

create stream ttl_test_02129(a int64, b string, d Date)
Engine=MergeTree partition by d order by a
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_ttl_recalculate_only = 1;

insert into ttl_test_02129 select number, '', '2021-01-01' from numbers(10);
alter stream ttl_test_02129 add column c int64 settings mutations_sync=2;

insert into ttl_test_02129 select number, '', '2021-01-01', 0 from numbers(10);
alter stream  ttl_test_02129 modify TTL (d + INTERVAL 1 MONTH) DELETE WHERE c=1 settings mutations_sync=2;

select * from ttl_test_02129 order by a, b, d, c;
drop stream ttl_test_02129;
