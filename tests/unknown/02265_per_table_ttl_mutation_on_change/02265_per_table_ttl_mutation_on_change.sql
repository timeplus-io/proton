drop stream if exists per_table_ttl_02265;
create stream per_table_ttl_02265 (key int, date Date, value string) engine=MergeTree() order by key;
insert into per_table_ttl_02265 values (1, today(), '1');

-- { echoOn }
alter stream per_table_ttl_02265 modify TTL date + interval 1 month;
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 1 month;
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 2 month;
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 2 month group by key set value = argMax(value, date);
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 2 month group by key set value = argMax(value, date);
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 2 month recompress codec(ZSTD(17));
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';
alter stream per_table_ttl_02265 modify TTL date + interval 2 month recompress codec(ZSTD(17));
select count() from system.mutations where database = current_database() and stream = 'per_table_ttl_02265';

-- { echoOff }
drop stream per_table_ttl_02265;
