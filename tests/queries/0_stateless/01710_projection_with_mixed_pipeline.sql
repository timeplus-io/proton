SET query_mode = 'table';
drop stream if exists t;

create stream t (x uint32) engine = MergeTree order by tuple() settings index_granularity = 8;
insert into t select number from numbers(100);
alter stream t add projection p (select uniqHLL12(x));
insert into t select number + 100 from numbers(100);
select uniqHLL12(x) from t settings allow_experimental_projection_optimization = 1, max_bytes_to_read=400, max_block_size=8; -- { serverError 307; }

drop stream if exists t;
