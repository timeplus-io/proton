-- Tags: long, replica

-- Testing basic functionality with compact parts
set replication_alter_partitions_sync = 2;
SET query_mode = 'table';
drop stream if exists mt_compact;

create stream mt_compact(a uint64, b uint64 DEFAULT a * a, s string, n nested(x uint32, y string), lc low_cardinality(string))
engine = ReplicatedMergeTree('/clickhouse/{database}/test_01201/mt_compact_replicated', '1')
order by a partition by a % 10
settings index_granularity = 8,
min_rows_for_wide_part = 10;

insert into mt_compact (a, s, n.y, lc) select number, to_string((number * 2132214234 + 5434543) % 2133443), ['a', 'b', 'c'], number % 2 ? 'bar' : 'baz' from numbers(90);

insert into mt_compact (a, s, n.x, lc) select number % 3, to_string((number * 75434535 + 645645) % 2133443), [1, 2], to_string(number) from numbers(5);

alter table mt_compact drop column n.y;
alter table mt_compact add column n.y array(string) DEFAULT ['qwqw'] after n.x;
select * from mt_compact order by a, s limit 10;
select '=====================';

alter table mt_compact update b = 42 where 1 SETTINGS mutations_sync = 2;

select * from mt_compact where a > 1 order by a, s limit 10;
select '=====================';

drop stream if exists mt_compact;
