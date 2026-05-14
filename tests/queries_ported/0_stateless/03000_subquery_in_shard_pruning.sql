-- Tags: shard

drop stream if exists 03000_subquery_in_shard_pruning;

create stream 03000_subquery_in_shard_pruning(id int, value int) settings shards=3, sharding_expr='to_int(id)';

insert into 03000_subquery_in_shard_pruning values (1, 10), (2, 20), (3, 30);

set optimize_skip_unused_shards = 1;
set force_optimize_skip_unused_shards = 2;
set optimize_skip_unused_shards_with_subqueries = 0;

-- Gating: with the new setting off we keep the old behavior and cannot prune the IN-subquery.
select count() from 03000_subquery_in_shard_pruning where id in (select 1); -- { serverError 507 }

set optimize_skip_unused_shards_with_subqueries = 1;

-- Basic bounded subquery pruning.
select count() from 03000_subquery_in_shard_pruning where id in (select 1);

-- Empty subquery result should prune to zero shards.
select count() from 03000_subquery_in_shard_pruning where id in (select 1 where 0);

-- NULLs are ignored for shard pruning, but the non-NULL key still gets rewritten.
set transform_null_in = 1;
select count() from 03000_subquery_in_shard_pruning where id in (select NULL union all select 1);
-- All-NULL results are not treated as a false contradiction, so they keep the fallback path.
select count() from 03000_subquery_in_shard_pruning where id in (select NULL union all select NULL); -- { serverError 507 }
set transform_null_in = 0;

-- If the ordered set is not available yet, shard pruning falls back to the old path.
set use_index_for_in_with_subqueries = 0;
select count() from 03000_subquery_in_shard_pruning where id in (select 1); -- { serverError 507 }
set use_index_for_in_with_subqueries = 1;

-- Limit is respected, so a larger IN-subquery keeps the existing no-pruning behavior.
select count() from 03000_subquery_in_shard_pruning where id in (select 1 union all select 2) settings optimize_skip_unused_shards_limit = 1; -- { serverError 507 }

-- NOT IN keeps the conservative fallback.
select count() from 03000_subquery_in_shard_pruning where id not in (select 1); -- { serverError 507 }

-- Multiple IN-subqueries are rewritten independently.
select count() from 03000_subquery_in_shard_pruning where id in (select 1) and value in (select 10);

drop stream if exists 03000_subquery_in_shard_pruning;
