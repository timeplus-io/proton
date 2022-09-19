SET query_mode = 'table';
drop stream if exists src;
drop stream if exists dst;
drop stream if exists mv1;
drop stream if exists mv2;

create stream src (key int) engine=Null();
create stream dst (key int) engine=Null();
create materialized view mv1 to dst as select * from src;
create materialized view mv2 to dst as select * from src;

insert into src select * from numbers(1e6) settings log_queries=1, max_untracked_memory=0, parallel_view_processing=1;
system flush logs;

-- { echo }
select view_name, read_rows, read_bytes, written_rows, written_bytes from system.query_views_log where startsWith(view_name, currentDatabase() || '.mv') order by view_name format Vertical;
select read_rows, read_bytes, written_rows, written_bytes from system.query_log where type = 'QueryFinish' and query_kind = 'Insert' and current_database = currentDatabase() format Vertical;
