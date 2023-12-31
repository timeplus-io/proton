SET query_mode = 'table';
drop stream if exists z;

create stream z (pk int64, d date, id uint64, c uint64) Engine MergeTree partition by d order by pk ;

insert into z  select number, '2021-10-24', intDiv (number, 10000), 1 from numbers(1000000);
optimize table z final;

alter stream z add projection pp (select id, sum(c) group by id);
alter stream z materialize projection pp settings mutations_sync=1;

SELECT name, partition, formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed, formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed, round(usize / size, 2) AS compr_rate, sum(rows) AS rows, count() AS part_count FROM system.projection_parts WHERE database = currentDatabase() and table = 'z' AND active GROUP BY name, partition ORDER BY size DESC;

drop stream z;
