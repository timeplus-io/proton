DROP VIEW IF EXISTS default.99004_v;
DROP VIEW IF EXISTS default.99004_v2;
DROP STREAM IF EXISTS default.99004_speed;

CREATE STREAM default.99004_speed (speed float64);

--- session with range comparision
CREATE MATERIALIZED VIEW default.99004_v AS
SELECT
  window_start AS start, window_end AS end, avg(speed)
FROM
  session(default.99004_speed, 1m, [speed > 50,speed < 50))
GROUP BY
  window_start, window_end;

--- session with start/end condition
CREATE MATERIALIZED VIEW default.99004_v2 AS
SELECT
  window_start AS start, window_end AS end, avg(speed)
FROM
  session(default.99004_speed, 1m, speed > 50, speed < 50)
GROUP BY
  window_start, window_end;

SHOW CREATE default.99004_v;

SHOW CREATE default.99004_v2;

--- error cases
SELECT window_start, window_end, avg(speed) FROM session(default.99004_speed, 1m, [speed > 50, speed > 60, speed < 50)) GROUP BY window_start, window_end; --- { clientError SYNTAX_ERROR }
SELECT window_start, window_end, avg(speed) FROM session(default.99004_speed, 1m, [speed > 50)) GROUP BY window_start, window_end; --- { clientError SYNTAX_ERROR }
SELECT window_start, window_end, avg(speed) FROM session(default.99004_speed, 1m, speed > 50, speed > 60, speed < 50) GROUP BY window_start, window_end; --- { serverError BAD_ARGUMENTS }
SELECT window_start, window_end, avg(speed) FROM session(default.99004_speed, 1m, speed > 50) GROUP BY window_start, window_end; --- { serverError MISSING_SESSION_KEY }

DROP VIEW default.99004_v;
DROP VIEW default.99004_v2;
DROP STREAM default.99004_speed;
