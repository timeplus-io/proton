-- https://github.com/timeplus-io/proton-enterprise/issues/12110
-- lag() OVER (PARTITION BY ...) inside a CTE wrapped by an outer WHERE
-- filter must keep per-partition-key state isolated. Before the fix,
-- tryMergeExpressions fused the substream-aware stateful child
-- ExpressionStep into the parent FilterStep, whose FilterTransform has
-- no per-SubstreamID cloning, so lag() state leaked across session_id.

DROP VIEW IF EXISTS 99922_mv_cte_where;
DROP STREAM IF EXISTS 99922_stream;

CREATE STREAM 99922_stream(event_time datetime64(3), session_id string, value int64)
  SETTINGS shards=1;

CREATE MATERIALIZED VIEW 99922_mv_cte_where AS
  WITH s AS (
    SELECT event_time, session_id, value,
           lag(value) OVER (PARTITION BY session_id) AS lag_v
    FROM 99922_stream
  )
  SELECT event_time AS detected_at, session_id, value, lag_v
  FROM s
  WHERE value > 1000
  SETTINGS checkpoint_interval=2;

SELECT sleep(2) FORMAT Null;

INSERT INTO 99922_stream(event_time, session_id, value, _tp_time) VALUES ('2026-05-11 18:00:01.000', 'vB', 480, '2026-05-11 18:00:01.000')('2026-05-11 18:00:04.000', 'vB', 5000, '2026-05-11 18:00:04.000')('2026-05-11 18:00:05.000', 'vA', 9500, '2026-05-11 18:00:05.000')('2026-05-11 18:00:06.000', 'vC', 3500, '2026-05-11 18:00:06.000')('2026-05-11 18:00:07.000', 'vC', 3200, '2026-05-11 18:00:07.000')('2026-05-11 18:00:08.000', 'vC', 3800, '2026-05-11 18:00:08.000');

SELECT sleep(3) FORMAT Null;

SELECT detected_at, session_id, value, lag_v
FROM table(99922_mv_cte_where)
ORDER BY detected_at;

DROP VIEW 99922_mv_cte_where;
DROP STREAM 99922_stream;
