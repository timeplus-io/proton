-- Hybrid global GROUP BY with EMIT ON UPDATE WITH BATCH should not re-emit
-- unchanged groups after pause/resume reloads checkpointed shared update-tracking
-- state from RocksDB (issue #11716).

drop view if exists ga_mv_99248;
drop stream if exists ga_s_99248;

create stream ga_s_99248(grp string, val int);

create materialized view ga_mv_99248 as
    select grp, count() as cnt, sum(val) as total
    from ga_s_99248
    group by grp
    emit on update with batch 1s
    settings default_hash_table = 'hybrid', max_hot_keys = 1, max_threads = 1, checkpoint_interval = 1
    STORAGE_SETTINGS flush_threshold_count = 1;

insert into ga_s_99248(grp, val) values('a', 10);
insert into ga_s_99248(grp, val) values('b', 20);
insert into ga_s_99248(grp, val) values('c', 30);
select sleep(3) format Null;

system pause materialized view ga_mv_99248;
system resume materialized view ga_mv_99248;
select sleep(2) format Null;

-- Only one existing group changes after recovery. Unchanged groups must not be
-- re-emitted from recovered updates state.
insert into ga_s_99248(grp, val) values('b', 5);
insert into ga_s_99248(grp, val) values('d', 40);
select sleep(2) format Null;

select grp, count() as emits, max(cnt) as max_cnt, max(total) as max_total
from table(ga_mv_99248)
group by grp
order by grp;

drop view if exists ga_mv_99248;
drop stream if exists ga_s_99248;
