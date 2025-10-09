-- Tags: disabled
-- https://github.com/timeplus-io/proton-enterprise/issues/9989

SET mutations_sync = 2;

DROP STREAM IF EXISTS t_projections_lwd;

CREATE STREAM t_projections_lwd (a uint32, b uint32, PROJECTION p (SELECT * ORDER BY b)) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_projections_lwd SELECT number, number FROM numbers(100);

-- LWD does not work, as expected
DELETE FROM t_projections_lwd WHERE a = 1; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = current_database() AND table = 't_projections_lwd' SYNC FORMAT Null;

-- drop projection
ALTER STREAM t_projections_lwd DROP projection p;

DELETE FROM t_projections_lwd WHERE a = 2;

SELECT count() FROM t_projections_lwd;

DROP STREAM t_projections_lwd;
