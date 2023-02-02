DROP STREAM IF EXISTS t_enum;
DROP STREAM IF EXISTS t_source;

CREATE STREAM t_enum(x enum8('hello' = 1, 'world' = 2)) ENGINE = TinyLog;
CREATE STREAM t_source(x nullable(string)) ENGINE = TinyLog;

INSERT INTO t_source (x) VALUES ('hello');
INSERT INTO t_enum(x) SELECT x from t_source WHERE x in ('hello', 'world');
SELECT * FROM t_enum;

DROP STREAM IF EXISTS t_enum;
DROP STREAM IF EXISTS t_source;
