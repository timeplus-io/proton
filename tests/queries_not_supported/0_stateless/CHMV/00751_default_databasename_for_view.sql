-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS test_00751;
USE test_00751;

DROP STREAM IF EXISTS t_00751;
DROP STREAM IF EXISTS t_mv_00751;
DROP STREAM IF EXISTS u_00751;
DROP STREAM IF EXISTS v_00751;

create stream t_00751
(
    date date,
    platform Enum8('a' = 0, 'b' = 1),
    app Enum8('a' = 0, 'b' = 1)
) ;

create stream u_00751 (app Enum8('a' = 0, 'b' = 1)) ;
create stream v_00751 (platform Enum8('a' = 0, 'b' = 1)) ;

INSERT INTO u_00751 VALUES ('b');
INSERT INTO v_00751 VALUES ('b');

CREATE MATERIALIZED VIEW t_mv_00751 ENGINE = MergeTree ORDER BY date
    AS SELECT date, platform, app FROM t_00751
    WHERE app = (SELECT min(app) from u_00751) AND platform = (SELECT (SELECT min(platform) from v_00751));

SHOW create stream test_00751.t_mv_00751 FORMAT TabSeparatedRaw;

USE default;
DETACH TABLE test_00751.t_mv_00751;
ATTACH TABLE test_00751.t_mv_00751;

INSERT INTO test_00751.t_00751 VALUES ('2000-01-01', 'a', 'a') ('2000-01-02', 'b', 'b');

INSERT INTO test_00751.u_00751 VALUES ('a');
INSERT INTO test_00751.v_00751 VALUES ('a');

INSERT INTO test_00751.t_00751 VALUES ('2000-01-03', 'a', 'a') ('2000-01-04', 'b', 'b');

SELECT * FROM test_00751.t_00751 ORDER BY date;
SELECT * FROM test_00751.t_mv_00751 ORDER BY date;

DROP STREAM test_00751.t_00751;
DROP STREAM test_00751.t_mv_00751;
DROP STREAM test_00751.u_00751;
DROP STREAM test_00751.v_00751;

DROP DATABASE test_00751;
