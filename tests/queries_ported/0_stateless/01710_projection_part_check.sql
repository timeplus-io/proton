drop stream if exists tp;

create stream tp (x int32, y int32, projection p (select x, y order by x)) engine = MergeTree order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

insert into tp select number, number from numbers(3);
insert into tp select number, number from numbers(5);

check table tp settings check_query_single_value_result=0;

drop stream tp;

create stream tp (p Date, k uint64, v1 uint64, v2 int64, projection p1 ( select p, sum(k), sum(v1), sum(v2) group by p) ) engine = MergeTree partition by to_YYYYMM(p) order by k settings min_bytes_for_wide_part = 0;

insert into tp (p, k, v1, v2) values ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

check table tp settings check_query_single_value_result=0;

drop stream tp;

drop stream if exists tp;
create stream tp (x int, projection p (select sum(x))) engine = MergeTree order by x settings min_rows_for_wide_part = 2, min_bytes_for_wide_part = 0;
insert into tp values (1), (2), (3), (4);
select part_type from system.parts where database = current_database() and table = 'tp';
select part_type from system.projection_parts where database = current_database() and table = 'tp';
check table tp settings check_query_single_value_result=0;
drop stream tp;
