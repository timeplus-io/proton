DROP VIEW IF EXISTS default.99004_v;

DROP STREAM IF EXISTS default.99004_speed;

CREATE STREAM default.99004_speed (speed float64);

CREATE MATERIALIZED VIEW default.99004_v AS
SELECT
  window_start AS start, window_end AS end, avg(speed)
FROM
  session(default.99004_speed, 1m, [speed > 50,speed < 50))
GROUP BY
  window_start, window_end;

SHOW CREATE default.99004_v;

DROP VIEW default.99004_v;

DROP STREAM default.99004_speed;
