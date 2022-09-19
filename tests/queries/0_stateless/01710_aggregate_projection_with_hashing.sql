set allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

SET query_mode = 'table';
drop stream if exists tp;

create stream tp (type int32, device UUID, cnt uint64) engine = MergeTree order by (type, device);
insert into tp select number%3, generateUUIDv4(), 1 from numbers(300);

alter stream tp add projection uniq_city_proj ( select type, uniq(cityHash64(device)), sum(cnt) group by type );
alter stream tp materialize projection uniq_city_proj settings mutations_sync = 1;

drop stream tp;
