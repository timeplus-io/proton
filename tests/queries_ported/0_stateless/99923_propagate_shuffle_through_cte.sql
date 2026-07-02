-- Issue #10505: SHUFFLE BY inside a CTE must propagate `shuffle_description` to the
-- outer aggregation, which now decides `keys_already_sharded` via a strict
-- key-coverage check (SHUFFLE BY keys must be a subset of GROUP BY keys).
-- The cases below confirm the check is correct in both directions across every
-- boundary that can carry or invalidate a shuffle: plain match/mismatch/superset,
-- nested shuffle, view-over-view, expression rebind, table function, ARRAY JOIN
-- (disjoint vs on the shuffle key), JOIN kinds (enrichment vs NULL-filling),
-- EXPLAIN-based elision proofs, and the streaming materialized-view path.

drop stream if exists test_99923_shuffle_cte;
drop stream if exists test_99923_shuffle_dim;

create stream test_99923_shuffle_cte(k int, g int, v int);
create stream test_99923_shuffle_dim(k int, d int);

insert into test_99923_shuffle_cte(k, g, v, _tp_time) values (1, 100, 10, '2024-01-01 00:00:00'), (1, 100, 20, '2024-01-01 00:00:01'), (2, 100, 30, '2024-01-01 00:00:02'), (2, 200, 40, '2024-01-01 00:00:03'), (3, 200, 50, '2024-01-01 00:00:04'), (3, 200, 60, '2024-01-01 00:00:05');

select sleep(2) format Null;

-- Case 1: SHUFFLE BY k, GROUP BY k (keys match → coversGroupByKeys = true; aggregation skips re-shard).
select '---match---';
with shuffled as
(
    select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2
)
select k, sum(v) as s from shuffled group by k order by k;

-- Case 2: SHUFFLE BY v, GROUP BY k (shuffle keys NOT a subset of GROUP BY → must re-shard;
-- if we incorrectly skipped, results across threads would be split / wrong).
select '---mismatch---';
with shuffled as
(
    select k, g, v from table(test_99923_shuffle_cte) shuffle by v settings max_threads=2
)
select k, sum(v) as s from shuffled group by k order by k;

-- Case 3: SHUFFLE BY k, GROUP BY (k, g) — GROUP BY is a superset of SHUFFLE BY keys → covered.
select '---superset---';
with shuffled as
(
    select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2
)
select k, g, sum(v) as s from shuffled group by k, g order by k, g;

-- Case 4: nested SHUFFLE BY with non-overlapping keys. The outer SHUFFLE BY g must still
-- fire even though the CTE already attached a Light shuffle on k — coarse "any upstream
-- shuffle" skipping here would mis-aggregate by leaving rows partitioned on the wrong key.
select '---nested-mismatch---';
with shuffled as
(
    select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2
)
select g, sum(v) as s from shuffled shuffle by g group by g order by g;

-- Case 5: view over view. The inner view owns the SHUFFLE BY, while the outer
-- view adds another query-plan boundary and an identity expression layer.
select '---view-over-view---';
drop view if exists test_99923_shuffle_view_outer;
drop view if exists test_99923_shuffle_view_inner;
create view test_99923_shuffle_view_inner as
    select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2;
create view test_99923_shuffle_view_outer as
    select k, g, v from test_99923_shuffle_view_inner;
select k, sum(v) as s from test_99923_shuffle_view_outer group by k order by k;
drop view if exists test_99923_shuffle_view_outer;
drop view if exists test_99923_shuffle_view_inner;

-- Case 6: rebinding the shuffle key name to another input column must clear
-- shuffle_description. Otherwise GROUP BY k would incorrectly consume a stream
-- partitioned by the old k instead of the new k (= g).
select '---expression-rebind---';
with shuffled as
(
    select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2
),
rebound as
(
    select g as k, v from shuffled
)
select k, sum(v) as s from rebound group by k order by k;

-- Case 7: table function source wrapped by a CTE.
select '---table-function---';
with shuffled as
(
    select number % 2 as k, number as v from numbers(6) shuffle by k settings max_threads=2
)
select k, sum(v) as s from shuffled group by k order by k;

-- Case 8: ARRAY JOIN on a non-shuffle-key column. Rows expand in place on the same stream
-- and k is untouched, so the Light shuffle survives and the outer GROUP BY k skips re-shard.
-- Correctness must hold regardless: each row is tripled by the 3-element array.
select '---array-join-disjoint---';
with shuffled as
(
    select k, [1, 2, 3] as tags, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2
)
select k, sum(v) as s from shuffled array join tags group by k order by k;

-- Case 9: ARRAY JOIN on the shuffle key itself. The key value changes from array to element,
-- so shuffle_description MUST be dropped and the outer GROUP BY must re-shard; if we wrongly
-- preserved it, same-element rows from different source arrays would split across streams and
-- mis-aggregate.
select '---array-join-is-shuffle-key---';
with shuffled as
(
    select [k, k % 2] as arr, v from table(test_99923_shuffle_cte) shuffle by arr settings max_threads=2
)
select arr as e, sum(v) as s from shuffled array join arr group by e order by e;

-- Case 10: EXPLAIN-based proof that the propagated shuffle_description actually elides the
-- redundant downstream SHUFFLE BY. count = 1 → outer SHUFFLE BY skipped (upstream partitioning
-- reused); count = 2 → it fired. This is the signal the result-only cases above cannot show,
-- since correctness holds either way.
select '---explain-cte-match---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-cte-mismatch---';
select count() from (explain with shuffled as (select k, g, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select g, sum(v) as s from shuffled shuffle by g group by g) where explain like '%LightShufflingStep%';
select '---explain-array-join-disjoint---';
select count() from (explain with shuffled as (select k, [1, 2] as tags, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled array join tags shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-array-join-is-shuffle-key---';
select count() from (explain with shuffled as (select [k, k % 2] as arr, v from table(test_99923_shuffle_cte) shuffle by arr settings max_threads=2) select arr as e, sum(v) as s from shuffled array join arr shuffle by e group by e) where explain like '%LightShufflingStep%';
-- FilterStep carries an ActionsDAG that mergeExpressions can fuse a projection into. A WHERE that
-- leaves k untouched preserves the shuffle (1); a fused projection that rebinds k (-k as k) keeps
-- the name but changes values, so it must be dropped and the outer SHUFFLE BY must fire (2).
select '---explain-filter-key-untouched---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from (select k, v from shuffled where v > 0) shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-filter-key-reprojected---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from (select -k as k, v from shuffled where v > 0) shuffle by k group by k) where explain like '%LightShufflingStep%';

-- Case 11: JOIN-kind gate. The left CTE is shuffled by k. Enrichment kinds (Inner/Left) keep every
-- left row carrying its original k, so the shuffle survives and the outer SHUFFLE BY k is elided (1).
-- Right/Full emit unmatched-right rows with NULL-filled left k, breaking same-key-same-stream, so
-- shuffle_description must be dropped and the outer SHUFFLE BY must fire (2).
select '---explain-join-inner---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled inner join table(test_99923_shuffle_dim) as d using (k) shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-join-left---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled left join table(test_99923_shuffle_dim) as d using (k) shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-join-right---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled right join table(test_99923_shuffle_dim) as d using (k) shuffle by k group by k) where explain like '%LightShufflingStep%';
select '---explain-join-full---';
select count() from (explain with shuffled as (select k, v from table(test_99923_shuffle_cte) shuffle by k settings max_threads=2) select k, sum(v) as s from shuffled full join table(test_99923_shuffle_dim) as d using (k) shuffle by k group by k) where explain like '%LightShufflingStep%';

drop stream if exists test_99923_shuffle_cte;
drop stream if exists test_99923_shuffle_dim;

-- Case 12: streaming path. Exercises executeStreamingAggregation's skip-reshard branch
-- through a materialized view fed by a multi-shard stream — the CTE's SHUFFLE BY actually
-- partitions across shards/threads, and the outer aggregation must consume that partitioning.
drop view if exists test_99923_streaming_mv;
drop stream if exists test_99923_streaming_src;

create stream test_99923_streaming_src(k int, v int) settings shards=2;

create materialized view test_99923_streaming_mv as
    with shuffled as (select k, v from test_99923_streaming_src shuffle by k settings max_threads=2)
    select k, sum(v) as s from shuffled group by k emit periodic 200ms;

select sleep(1) format Null;

insert into test_99923_streaming_src(k, v) values (1, 10), (1, 20), (2, 30), (2, 40), (3, 50);

select sleep(3) format Null;

select '---streaming---';
-- sum() is monotonic across updates → max() pulls the final accumulated value per key.
select k, max(s) as s from table(test_99923_streaming_mv) group by k order by k;

drop view if exists test_99923_streaming_mv;
drop stream if exists test_99923_streaming_src;

-- Case 13: streaming mismatch (negative of Case 12). The CTE shuffles by v, which does NOT cover the
-- outer GROUP BY k, so executeStreamingAggregation must re-shard. If it wrongly skipped, same-k rows
-- partitioned by v would split across streams into independent partial sums that never merge, and the
-- outer max(s) would surface a partial sum instead of the full per-key total.
drop view if exists test_99923_streaming_mv_mismatch;
drop stream if exists test_99923_streaming_src_mismatch;

create stream test_99923_streaming_src_mismatch(k int, v int) settings shards=2;

create materialized view test_99923_streaming_mv_mismatch as
    with shuffled as (select k, v from test_99923_streaming_src_mismatch shuffle by v settings max_threads=2)
    select k, sum(v) as s from shuffled group by k emit periodic 200ms;

select sleep(1) format Null;

insert into test_99923_streaming_src_mismatch(k, v) values (1, 10), (1, 20), (2, 30), (2, 40), (3, 50);

select sleep(3) format Null;

select '---streaming-mismatch---';
select k, max(s) as s from table(test_99923_streaming_mv_mismatch) group by k order by k;

drop view if exists test_99923_streaming_mv_mismatch;
drop stream if exists test_99923_streaming_src_mismatch;

-- Case 14: substream (PARTITION BY) propagation across a CTE boundary — the substream-path mirror of
-- the Light-shuffle EXPLAIN proofs in Case 10, where isShuffledBy requires Kind::Substream coverage.
-- A CTE that PARTITION BY k lets the outer PARTITION BY k be elided (1); PARTITION BY v does not cover
-- the outer key, so the outer SubstreamShufflingStep must fire (2).
drop stream if exists test_99923_substream_src;
create stream test_99923_substream_src(k int, v int);
select '---explain-substream-cte-match---';
select count() from (explain with shuffled as (select k, v from test_99923_substream_src partition by k) select k, sum(v) as s from shuffled partition by k group by k) where explain like '%SubstreamShufflingStep%';
select '---explain-substream-cte-mismatch---';
select count() from (explain with shuffled as (select k, v from test_99923_substream_src partition by v) select k, sum(v) as s from shuffled partition by k group by k) where explain like '%SubstreamShufflingStep%';
drop stream if exists test_99923_substream_src;
