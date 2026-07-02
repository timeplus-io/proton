-- Regression for EMIT ON UPDATE on (-1, +1) consecutive pairs. Two invariants:
--   1. Trailing +1 must stamp a watermark even when filtered to empty (issue #10567).
--   2. Leading -1 must NOT stamp — enforced by setConsecutiveDataFlag also
--      setting avoidWatermark. If it stamps, the downstream aggregator
--      finalizes on the retract alone and leaks an intermediate state the
--      user's UPDATE never asked for.
--
-- Four shapes:
--   * no partition, +1 filtered empty
--   * partition by pk, +1 filtered empty
--   * partition by non-pk, cross-substream PK move
--   * partition by pk, both halves non-empty (non-PK field update)

-- ====================================================================
-- no partition, +1 filtered empty
-- ====================================================================

select 'no partition, +1 filtered empty' format CSV;

drop view if exists 99923_mv1;
drop stream if exists 99923_t1;
drop stream if exists 99923_t2;

create mutable stream 99923_t1(k1 string, k2 string, v1 int, v2 int) primary key (k1, k2);
create mutable stream 99923_t2(k1 string, k2 string, v1 int, v2 int, v3 int) primary key (k1, k2);

create materialized view 99923_mv1 as
with t1_vw as (select * from 99923_t1 where v1 > 0 and k1 != 'segment'),
     t2_vw as (select * from 99923_t2 where v3 < 0 and k1 != 'segment')
select
    case when empty(t1_vw.k1) and empty(t1_vw.k2) then t2_vw.k1 else t1_vw.k1 end as k1,
    case when empty(t1_vw.k1) and empty(t1_vw.k2) then t2_vw.k2 else t1_vw.k2 end as k2,
    min(t1_vw.v1) as v1,
    max(t1_vw.v2) as v2,
    sum(t2_vw.v3) as v3
from t1_vw
full outer join t2_vw on (t1_vw.k1 = t2_vw.k1 and t1_vw.k2 = t2_vw.k2)
group by k1, k2
emit on update
STORAGE_SETTINGS flush_threshold_count = 1;

select sleep(2) format Null;

insert into 99923_t1(k1, k2, v1, v2) values ('k1', 'k1', 1, 2);
select sleep(2) format Null;

-- v1=-1 row is filtered by `v1 > 0`, so the +1 partner is an empty chunk.
insert into 99923_t1(k1, k2, v1, v2) values ('k1', 'k1', -1, 2);
select sleep(3) format Null;

select k1, k2, v1, v2, v3 from table(99923_mv1) order by _tp_sn;

drop view 99923_mv1;
drop stream 99923_t1;
drop stream 99923_t2;

-- ====================================================================
-- partition by pk, +1 filtered empty
-- ====================================================================

select 'partition by pk, +1 filtered empty' format CSV;

drop view if exists 99923_mv2;
drop stream if exists 99923_t3;

create mutable stream 99923_t3(k string, v int, v2 int) primary key k;

create materialized view 99923_mv2 as
select k, min(v) as min_v, max(v2) as max_v2
from (select * from 99923_t3 where v > 0)
partition by k
group by k
emit on update
STORAGE_SETTINGS flush_threshold_count = 1;

select sleep(2) format Null;

insert into 99923_t3(k, v, v2) values ('k1', 1, 10);
select sleep(2) format Null;

-- v=-1 row is filtered, so the +1 partner reaches WatermarkTransformWithSubstream
-- as an empty heartbeat with its substream id cleared.
insert into 99923_t3(k, v, v2) values ('k1', -1, 20);
select sleep(3) format Null;

select k, min_v, max_v2 from table(99923_mv2) order by _tp_sn;

drop view 99923_mv2;
drop stream 99923_t3;

-- ====================================================================
-- partition by non-pk, cross-substream PK move
-- ====================================================================

select 'partition by non-pk, cross-substream PK move' format CSV;

drop view if exists 99923_mv3;
drop stream if exists 99923_t4;

create mutable stream 99923_t4(k string, v int, payload int) primary key k;

create materialized view 99923_mv3 as
select v, max(payload) as max_p, count() as cnt
from 99923_t4
partition by v
group by v
emit on update
STORAGE_SETTINGS flush_threshold_count = 1;

select sleep(2) format Null;

insert into 99923_t4(k, v, payload) values ('a', 1, 10);
select sleep(2) format Null;

-- Update PK 'a' from v=1 to v=2: retract on substream v=1 then update on
-- substream v=2. Both chunks have rows but land on different substreams, so the
-- v=1 watermark must be released via the cross-substream heartbeat path.
insert into 99923_t4(k, v, payload) values ('a', 2, 10);
select sleep(3) format Null;

select v, max_p, cnt from table(99923_mv3) order by _tp_sn;

drop view 99923_mv3;
drop stream 99923_t4;

-- ====================================================================
-- partition by pk, both halves non-empty (non-PK field update)
-- ====================================================================

select 'partition by pk, both halves non-empty' format CSV;

drop view if exists 99923_mv4;
drop stream if exists 99923_t5;

create mutable stream 99923_t5(k string, v int) primary key k;

create materialized view 99923_mv4 as
select k, sum(v) as s, count() as c
from 99923_t5
partition by k
group by k
emit on update
STORAGE_SETTINGS flush_threshold_count = 1;

select sleep(2) format Null;

insert into 99923_t5(k, v) values ('a', 5);
select sleep(2) format Null;

-- Update PK 'a' value 5 -> 10. (-1 v=5) and (+1 v=10) both non-empty on the
-- same substream k='a'. The leading -1 must not stamp; if it does, the
-- aggregator finalizes mid-pair and leaks an intermediate (a, 0, 0).
insert into 99923_t5(k, v) values ('a', 10);
select sleep(3) format Null;

select k, s, c from table(99923_mv4) order by _tp_sn;

drop view 99923_mv4;
drop stream 99923_t5;
