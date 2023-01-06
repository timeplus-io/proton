-- Tags: no-ordinary-database
SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS test_00609;
DROP STREAM IF EXISTS test_mv_00609;

create stream test_00609 (a int8) engine=Memory;

insert into test_00609(a) values (1);
select sleep(3);
create materialized view test_mv_00609 uuid '00000609-1000-4000-8000-000000000001' Engine=MergeTree(date, (a), 8192) populate as select a, to_date('2000-01-01') date from test_00609;

select * from test_mv_00609; -- OK
select * from test_mv_00609 where a in (select a from test_mv_00609); -- EMPTY (bug)
select * from ".inner_id.00000609-1000-4000-8000-000000000001" where a in (select a from test_mv_00609); -- OK

DROP STREAM test_00609;
DROP STREAM test_mv_00609;
