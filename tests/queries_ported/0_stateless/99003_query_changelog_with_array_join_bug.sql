drop view if exists default.mv_99003;
drop stream if exists default.vk_99003;

create stream default.vk_99003 (i int, v1 float, v2 float) primary key i settings mode='versioned_kv';
create materialized view default.mv_99003 as with cte as (select i, [v1, v2] as value_arr from default.vk_99003) select i, min(value) as min_v, max(value) as max_v from changelog(cte) array join value_arr as value group by i emit periodic 1s storage_settings flush_threshold_ms=1;

select sleep(3) format Null;
insert into default.vk_99003(i, v1, v2) values (1, 1.1, 2.2);
select sleep(3) format Null;
select sleep(3) format Null;

select i, min_v, max_v from table(default.mv_99003);

drop view if exists default.mv_99003;
drop stream if exists default.vk_99003;
