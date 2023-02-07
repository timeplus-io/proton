-- https://github.com/ClickHouse/ClickHouse/issues/37900

drop stream if exists t;
drop stream if exists t_mv;
create stream t (a uint64) Engine = Null;
create materialized view t_mv Engine = Null AS select now() as ts, max(a) from t group by ts;

insert into t select * from numbers_mt(10e6) settings max_threads = 16, max_insert_threads=16;
system flush logs;

select arrayUniq(thread_ids)>=16 from system.query_log where
    event_date >= yesterday() and
    current_database = current_database() and
    type = 'QueryFinish' and
    startsWith(query, 'insert');
