-- Tags: no-parallel
SET query_mode = 'table';
drop dictionary if exists default.test_dict_01051_d;
drop stream if exists default.test_01051_d;
drop stream if exists default.test_view_01051_d;

create stream default.test_01051_d (key uint64, value string) engine = MergeTree order by key;
create view default.test_view_01051_d (key uint64, value string) as select k2 + 1 as key, v2 || '_x' as value from (select key + 2 as k2, value || '_y' as v2 from default.test_01051_d);

insert into default.test_01051_d values (1, 'a');

create dictionary default.test_dict_01051_d (key uint64, value string) primary key key source(clickhouse(host 'localhost' port '9000' user 'default' password '' db 'default' table 'test_view_01051_d')) layout(flat()) lifetime(100500);

select dictGet('default.test_dict_01051_d', 'value', to_uint64(4));

drop dictionary if exists default.test_dict_01051_d;
drop stream if exists default.test_01051_d;
drop stream if exists default.test_view_01051_d;
