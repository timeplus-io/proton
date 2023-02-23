-- From https://github.com/ClickHouse/ClickHouse/issues/41814
drop stream if exists test;

create stream test(a uint64, m uint64, d DateTime) engine MergeTree partition by to_YYYYMM(d) order by (a, m, d);

insert into test select number, number, '2022-01-01 00:00:00' from numbers(1000000);

select count() from test where a = (select to_uint64(1) where 1 = 2) settings enable_early_constant_folding = 0, force_primary_key = 1;

drop stream test;

-- From https://github.com/ClickHouse/ClickHouse/issues/34063
drop stream if exists test_null_filter;

create stream test_null_filter(key uint64, value uint32) engine MergeTree order by key;

insert into test_null_filter select number, number from numbers(10000000);

select count() from test_null_filter where key = null and value > 0 settings force_primary_key = 1;

drop stream test_null_filter;
