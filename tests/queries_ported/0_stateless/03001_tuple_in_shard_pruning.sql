-- Tags: shard
-- Regression test for tuple-IN shard pruning.
-- Literal `(c1, c2) IN ((a, b), (c, d))` should narrow to the same shards as
-- the equivalent `c1=a AND c2 IN (b)` form. Before evaluateConstantExpression.cpp
-- learned to decompose tuple-IN into a disjunction of conjunctions, the LHS
-- `tuple_cast(c1, c2)` did not match `ASTIdentifier` and shard pruning silently
-- gave up, scanning every shard.

drop stream if exists 03001_tuple_in_shard_pruning;

create stream 03001_tuple_in_shard_pruning (chain_id uint16, address string, v uint32)
ENGINE = Stream(8, city_hash64((chain_id, address)))
PRIMARY KEY (chain_id, address)
ORDER BY    (chain_id, address)
SETTINGS index_granularity = 64;

insert into 03001_tuple_in_shard_pruning(chain_id, address, v)
select (number % 2 + 1)::uint16, concat('addr_', to_string(number)), number::uint32 from numbers(2000);

select sleep(3) format Null;

set optimize_skip_unused_shards = 1;

-- Correctness across forms: tuple-IN, flat-IN, and the equivalent OR-of-AND
-- must all produce identical counts.
select count() from table(03001_tuple_in_shard_pruning) where chain_id = 1 and address in ('addr_42', 'addr_88');
-- EXPLAIN PIPELINE proves the flat-IN baseline reads only the two shards holding
-- these two sharding-key values.
select count() from (explain pipeline select count() from table(03001_tuple_in_shard_pruning) where chain_id = 1 and address in ('addr_42', 'addr_88')) where explain like '%MergeTreeSelect%';
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_88'));
-- Tuple-IN should prune to the same two shards as the flat-IN baseline.
select count() from (explain pipeline select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_88'))) where explain like '%MergeTreeSelect%';
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1::uint16, 'addr_42'), (1::uint16, 'addr_88'));
select count() from table(03001_tuple_in_shard_pruning) where (chain_id = 1 and address in ('addr_42', 'addr_88'));

-- Mixed chain_ids: tuple-IN with cross-chain rows.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (2, 'addr_88'), (1, 'addr_222'));

-- An always-false constant predicate combined with tuple-IN returns zero rows.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42')) and 0;

-- A non-matching tuple returns zero (verifies shard pruning didn't accidentally
-- prune a shard that contained matching rows).
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'no_such_addr'), (2, 'no_such_addr'));

-- The limit is respected; if the IN-set exceeds it, shard pruning falls back
-- to the no-prune path but correctness is preserved.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_88'), (1, 'addr_222')) settings optimize_skip_unused_shards_limit = 1;
select count() from (explain pipeline select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_88'), (1, 'addr_222')) settings optimize_skip_unused_shards_limit = 1) where explain like '%MergeTreeSelect%';

-- A non-literal value in a tuple row (here a function call) triggers the safe
-- fallback in analyzeFunction — shard pruning gives up but the query still
-- returns the right rows.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, concat('addr_', '42')), (1, 'addr_88'));

-- Duplicate inner tuples on the RHS must be deduped before they consume the
-- shard-pruning limit budget. Without dedup, 3 conjunctions exceed `limit=2`
-- and the optimization silently falls back to scanning every shard.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_42'), (1, 'addr_88')) settings optimize_skip_unused_shards_limit = 2;
select count() from (explain pipeline select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_42'), (1, 'addr_88')) settings optimize_skip_unused_shards_limit = 2) where explain like '%MergeTreeSelect%';

-- And the row count itself is invariant under duplication on the RHS.
select count() from table(03001_tuple_in_shard_pruning) where (chain_id, address) in ((1, 'addr_42'), (1, 'addr_42'));

drop stream if exists 03001_tuple_in_shard_pruning;
