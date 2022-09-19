-- Testing basic functionality with compact parts
set mutations_sync = 2;
SET query_mode = 'table';
drop stream if exists mt_compact;

create stream mt_compact(a uint64, b uint64 DEFAULT a * a, s string, n nested(x uint32, y string), lc LowCardinality(string))
engine = MergeTree
order by a partition by a % 10
settings index_granularity = 8,
min_bytes_for_wide_part = 0,
min_rows_for_wide_part = 10;

insert into mt_compact (a, s, n.y, lc) select number, to_string((number * 2132214234 + 5434543) % 2133443), ['a', 'b', 'c'], number % 2 ? 'bar' : 'baz' from numbers(90);

select * from mt_compact order by a limit 10;
select '=====================';

select distinct part_type from system.parts where database = currentDatabase() and table = 'mt_compact' and active;

insert into mt_compact (a, s, n.x, lc) select number % 3, to_string((number * 75434535 + 645645) % 2133443), [1, 2], to_string(number) from numbers(5);

optimize table mt_compact final;

select part_type, count() from system.parts where database = currentDatabase() and table = 'mt_compact' and active group by part_type order by part_type;
select * from mt_compact order by a, s limit 10;
select '=====================';

alter table mt_compact drop column n.y;
alter table mt_compact add column n.y array(string) DEFAULT ['qwqw'] after n.x;
select * from mt_compact order by a, s limit 10;
select '=====================';

alter table mt_compact update b = 42 where 1;

select * from mt_compact where a > 1 order by a, s limit 10;
select '=====================';

drop stream if exists mt_compact;
