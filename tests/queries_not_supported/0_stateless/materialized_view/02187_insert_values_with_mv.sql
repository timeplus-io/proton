CREATE STREAM IF NOT EXISTS data_a_02187 (a int64) ENGINE=Memory;
CREATE STREAM IF NOT EXISTS data_b_02187 (a int64) ENGINE=Memory;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 INTO data_b_02187 AS Select sleep_each_row(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv2 INTO data_b_02187 AS Select sleep_each_row(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv3 INTO data_b_02187 AS Select sleep_each_row(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv4 INTO data_b_02187 AS Select sleep_each_row(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv5 INTO data_b_02187 AS Select sleep_each_row(0.05) as a FROM data_a_02187;

-- INSERT USING VALUES
INSERT INTO data_a_02187 VALUES (1);
-- INSERT USING STREAM
INSERT INTO data_a_02187 SELECT * FROM system.one;
SYSTEM FLUSH LOGS;

SELECT 'VALUES', query_duration_ms >= 250
FROM system.query_log
WHERE
      current_database = current_database()
  AND event_date >= yesterday()
  AND query LIKE '-- INSERT USING VALUES%'
  AND type = 'QueryFinish'
LIMIT 1;

SELECT 'TABLE', query_duration_ms >= 250
FROM system.query_log
WHERE
        current_database = current_database()
  AND event_date >= yesterday()
  AND query LIKE '-- INSERT USING VALUES%'
  AND type = 'QueryFinish'
LIMIT 1;

WITH
    (
        SELECT initial_query_id
        FROM system.query_log
        WHERE
                current_database = current_database()
          AND event_date >= yesterday()
          AND query LIKE '-- INSERT USING VALUES%'
        LIMIT 1
    ) AS q_id
SELECT 'VALUES', view_duration_ms >= 50
FROM system.query_views_log
WHERE initial_query_id = q_id;

WITH
(
    SELECT initial_query_id
    FROM system.query_log
    WHERE
            current_database = current_database()
      AND event_date >= yesterday()
      AND query LIKE '-- INSERT USING STREAM%'
    LIMIT 1
) AS q_id
SELECT 'TABLE', view_duration_ms >= 50
FROM system.query_views_log
WHERE initial_query_id = q_id;
