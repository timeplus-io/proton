DROP VIEW IF EXISTS 99923_mv;
DROP STREAM IF EXISTS 99923_stream;

CREATE STREAM 99923_stream(id int, val int);

CREATE MATERIALIZED VIEW 99923_mv AS
  SELECT id, sum(val) AS total
  FROM 99923_stream
  GROUP BY id
  SETTINGS checkpoint_settings='offsets_only=true', checkpoint_interval=1;

SELECT sleep(2) FORMAT Null;

INSERT INTO 99923_stream(id, val) VALUES (1,10)(2,20);
SELECT sleep(2) FORMAT Null;

SYSTEM PAUSE MATERIALIZED VIEW 99923_mv;
SYSTEM RESUME MATERIALIZED VIEW 99923_mv;
SELECT sleep(2) FORMAT Null;

INSERT INTO 99923_stream(id, val) VALUES (3,30);
SELECT sleep(2) FORMAT Null;

SELECT id, total FROM table(99923_mv) ORDER BY id, total;

DROP VIEW 99923_mv;
DROP STREAM 99923_stream;

SELECT 'done';
