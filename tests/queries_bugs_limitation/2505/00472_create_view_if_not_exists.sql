SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS t_00472;
DROP STREAM IF EXISTS mv_00472;
DROP STREAM IF EXISTS `.inner.mv_00472`;

create stream t_00472 (x uint8) ENGINE = Null;
CREATE VIEW IF NOT EXISTS mv_00472 AS SELECT * FROM t_00472;

DROP STREAM t_00472;
DROP STREAM mv_00472;
