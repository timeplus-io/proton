-- Regression: EMIT ON UPDATE over a multi-shard versioned_kv stream
-- must not emit spurious retracted intermediate rows.
--
-- On shards>1, per-shard ChangelogConvert emits an update as an adjacent
-- (-1, +1) consecutive pair, but the shards->1 merge (ShrinkResizeProcessor)
-- used to interleave idle shards' broadcast heartbeats between the pair. The
-- downstream WatermarkTransformWithSubstream then finalized the half-applied
-- retract, leaking a [0, 0, nan, 0, <key>] row (count=0) alongside the correct
-- update. ShrinkResizeProcessor now pins the input mid-pair, so no foreign
-- chunk splits a (-1, +1) pair and no spurious row is produced.
--
-- The spurious row is uniquely identified by count()=0 (a live versioned_kv key
-- always has a current value, so a valid group never has count 0).
--
-- NOTE: this is an end-to-end SMOKE. The interleave is provoked by sleep-paced
-- inserts, so it is only near-deterministic and can pass even on the buggy code.
-- The binding, deterministic regression proof is the unit test
-- ShrinkResizeConsecutive.PairNotSplitByForeignHeartbeat (gtest_shrink_resize_consecutive).

drop view if exists 99926_mv;
drop stream if exists 99926_vkv;

create stream 99926_vkv(i float, k2 string)
primary key k2
settings mode = 'versioned_kv', shards = 3;

create materialized view 99926_mv as
select max(i) as mx, min(i) as mn, avg(i) as av, count() as c, k2
from 99926_vkv
partition by k2
group by k2
emit on update
storage_settings flush_threshold_count = 1;

select sleep(2) format Null;

-- Rotate inserts over 3 keys (near-deterministic trigger).
insert into 99926_vkv(i, k2) values (1, 'a');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (2, 'b');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (3, 'c');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (4, 'a');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (5, 'b');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (6, 'c');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (7, 'a');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (8, 'b');
select sleep(1) format Null;
insert into 99926_vkv(i, k2) values (9, 'c');
select sleep(3) format Null;

-- No spurious count()=0 (nan-avg) rows must be materialized.
select count() as spurious_rows from table(99926_mv) where c = 0;

-- Positive guard: the MV actually processed all inserts — each key reached its final
-- value (a->7, b->8, c->9). Without this, a "0 spurious rows" pass on an empty/unprocessed
-- MV would be meaningless.
select k2, max(mx) as final_v from table(99926_mv) group by k2 order by k2;

drop view 99926_mv;
drop stream 99926_vkv;
