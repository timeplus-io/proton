DROP STREAM IF EXISTS t_enum;
DROP STREAM IF EXISTS t_source;

create stream t_enum(x enum8('hello' = 1, 'world' = 2)) ;
create stream t_source(x nullable(string)) ;

INSERT INTO t_source (x) VALUES ('hello');
INSERT INTO t_enum(x) SELECT x from t_source WHERE x in ('hello', 'world');
SELECT * FROM t_enum;

DROP STREAM IF EXISTS t_enum;
DROP STREAM IF EXISTS t_source;
