DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS t_v;

create stream t ( a string ) ();
CREATE VIEW t_v AS SELECT * FROM t;
SET output_format_write_statistics = 0;
SELECT * FROM t_v FORMAT JSON SETTINGS extremes = 1;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS t_v;
