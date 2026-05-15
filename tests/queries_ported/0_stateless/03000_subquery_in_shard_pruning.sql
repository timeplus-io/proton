-- Tags: shard
-- Regression test for optimize_skip_unused_shards_with_subqueries.
-- For local Stream engine, force_optimize_skip_unused_shards does not raise 507
-- (that is a Distributed-only enforcement), so this test validates correctness
-- across the new code path. A separate explain-based test could verify pruning.

drop stream if exists 03000_subquery_in_shard_pruning;

create stream 03000_subquery_in_shard_pruning(id int, value int) settings shards=3, sharding_expr='to_int(id)';

insert into 03000_subquery_in_shard_pruning(id, value) values (1, 10);
insert into 03000_subquery_in_shard_pruning(id, value) values (2, 20);
insert into 03000_subquery_in_shard_pruning(id, value) values (3, 30);

select sleep(3) format Null;

set optimize_skip_unused_shards = 1;
set optimize_skip_unused_shards_with_subqueries = 0;

-- Gating: with the new setting off we keep the old behavior.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1);

set optimize_skip_unused_shards_with_subqueries = 1;

-- Basic bounded subquery pruning preserves correctness.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1);

-- Empty subquery result.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1 where 0);

-- NULLs are ignored for shard pruning, but the non-NULL key still gets rewritten.
set transform_null_in = 1;
select count() from table(03000_subquery_in_shard_pruning) where id in (select cast(NULL, 'nullable(int32)') union all select 1);
-- All-NULL results fall back to the no-prune path while keeping correctness.
select count() from table(03000_subquery_in_shard_pruning) where id in (select cast(NULL, 'nullable(int32)') union all select cast(NULL, 'nullable(int32)'));
set transform_null_in = 0;

-- If the ordered set is not available yet, shard pruning falls back to the old path,
-- but the query still returns the correct count.
set use_index_for_in_with_subqueries = 0;
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1);
set use_index_for_in_with_subqueries = 1;

-- Limit is respected: a larger IN-subquery keeps the existing no-pruning behavior.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1 union all select 2) settings optimize_skip_unused_shards_limit = 1;

-- NOT IN keeps the conservative fallback.
select count() from table(03000_subquery_in_shard_pruning) where id not in (select 1);

-- Multiple IN-subqueries are rewritten independently.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1) and value in (select 10);

drop stream if exists 03000_subquery_in_shard_pruning;
