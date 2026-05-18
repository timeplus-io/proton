-- Tags: shard
-- Regression test for optimize_skip_unused_shards_with_subqueries.
-- For local Stream engine, force_optimize_skip_unused_shards does not raise 507
-- (that is a Distributed-only enforcement), so this test validates correctness
-- across the new code path and EXPLAIN PIPELINE verifies the selected shards.

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
-- With the subquery rewrite disabled, the IN-subquery cannot drive shard pruning
-- and the pipeline still reads all three Stream shards.
select count() from (explain pipeline select count() from table(03000_subquery_in_shard_pruning) where id in (select 1)) where explain like '%MergeTreeSelect%';

set optimize_skip_unused_shards_with_subqueries = 1;

-- Basic bounded subquery pruning preserves correctness.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1);
-- The same bounded subquery is materialized early enough for shard pruning:
-- id=1 maps to one Stream shard, so the pipeline has one MergeTreeSelect.
select count() from (explain pipeline select count() from table(03000_subquery_in_shard_pruning) where id in (select 1)) where explain like '%MergeTreeSelect%';

-- Empty subquery result.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1 where 0);

-- Any NULL in the subquery result forces a conservative fallback: dropping NULLs
-- from the rewritten literal IN would mis-prune shards holding NULL-keyed rows
-- (relevant for nullable sharding keys with transform_null_in=1). Result must
-- stay correct either way.
set transform_null_in = 1;
select count() from table(03000_subquery_in_shard_pruning) where id in (select cast(NULL, 'nullable(int32)') union all select 1);
select count() from table(03000_subquery_in_shard_pruning) where id in (select cast(NULL, 'nullable(int32)') union all select cast(NULL, 'nullable(int32)'));
set transform_null_in = 0;

-- If the ordered set is not available yet, shard pruning falls back to the old path,
-- but the query still returns the correct count.
set use_index_for_in_with_subqueries = 0;
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1);
set use_index_for_in_with_subqueries = 1;

-- Limit is respected: a larger IN-subquery keeps the existing no-pruning behavior.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1 union all select 2) settings optimize_skip_unused_shards_limit = 1;
select count() from (explain pipeline select count() from table(03000_subquery_in_shard_pruning) where id in (select 1 union all select 2) settings optimize_skip_unused_shards_limit = 1) where explain like '%MergeTreeSelect%';

-- NOT IN keeps the conservative fallback.
select count() from table(03000_subquery_in_shard_pruning) where id not in (select 1);

-- Multiple IN-subqueries are rewritten independently.
select count() from table(03000_subquery_in_shard_pruning) where id in (select 1) and value in (select 10);

-- An empty IN-subquery in a non-conjunctive position must not short-circuit shard
-- selection to {}. With the old "no or/not anywhere" check, `if(...)` and `multi_if(...)`
-- slipped through and returned 0 instead of the correct count.
select count() from table(03000_subquery_in_shard_pruning) where if(value = 10, 1, id in (select 1 where 0));
select count() from table(03000_subquery_in_shard_pruning) where multi_if(value = 10, 1, id in (select 1 where 0), 99, 1);
-- Empty IN inside a `tuple` element must not zero out the shard set either.
select count() from table(03000_subquery_in_shard_pruning) where tuple_element((1, id in (select 1 where 0)), 1) = 1;

-- Historical IN-subquery against the same stream should match every row and read every shard.
select count() from table(03000_subquery_in_shard_pruning) where id in (select id from table(03000_subquery_in_shard_pruning));

-- Early shard-pruning materialization must not suppress normal subquery-depth validation.
select count() from table(03000_subquery_in_shard_pruning) where id in (select id from (select id from (select 1 as id))) settings max_subquery_depth = 2; -- { serverError 162 }

-- This subquery is bounded and materializes to an empty set, but the outer
-- streaming query must remain continuous instead of finishing on a finite empty source.
select id from 03000_subquery_in_shard_pruning where id in (select 1 where 0) limit 1 settings max_execution_time = 3; -- { serverError 159 }

-- This subquery reads a live stream, so it cannot be materialized into a fixed set;
-- it must fall back to the standard NOT_IMPLEMENTED path, not crash.
select count() from table(03000_subquery_in_shard_pruning) where id in (select id from 03000_subquery_in_shard_pruning); -- { serverError 48 }

drop stream if exists 03000_subquery_in_shard_pruning;
