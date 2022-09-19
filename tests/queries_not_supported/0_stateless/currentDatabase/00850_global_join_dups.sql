-- Tags: global

DROP STREAM IF EXISTS t_local;
DROP STREAM IF EXISTS t1_00850;
DROP STREAM IF EXISTS t2_00850;

create stream t_local (dummy uint8) ;
create stream t1_00850 (dummy uint8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_local');
create stream t2_00850 (dummy uint8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_local');

INSERT INTO t_local VALUES (1);

SET joined_subquery_requires_alias = 0;

SELECT * FROM t1_00850
GLOBAL INNER JOIN
(
    SELECT *
    FROM ( SELECT * FROM t2_00850 )
    INNER JOIN ( SELECT * FROM t1_00850 )
    USING dummy
) USING dummy;

-- query from fuzzer
SELECT toDateTime64(to_string(to_string('0000-00-00 00:00:000000-00-00 00:00:00', toDateTime64(toDateTime64('655.36', -2, NULL)))), NULL) FROM t1_00850 GLOBAL INNER JOIN (SELECT toDateTime64(toDateTime64('6553.6', '', NULL), NULL), * FROM (SELECT * FROM t2_00850) INNER JOIN (SELECT toDateTime64('6553.7', 1024, NULL), * FROM t1_00850) USING (dummy)) USING (dummy);

SELECT to_string('0000-00-00 00:00:000000-00-00 00:00:00', toDateTime64(toDateTime64('655.36', -2, NULL)));

DROP STREAM t_local;
DROP STREAM t1_00850;
DROP STREAM t2_00850;


SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *
    FROM ( SELECT dummy FROM remote('127.0.0.2', system.one) ) t1_00850
    GLOBAL INNER JOIN ( SELECT dummy FROM remote('127.0.0.3', system.one) ) t2_00850
    USING dummy
) USING dummy;

SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *, dummy
    FROM ( SELECT dummy FROM remote('127.0.0.2', system.one) ) t1_00850
    GLOBAL INNER JOIN ( SELECT dummy FROM remote('127.0.0.3', system.one) ) t2_00850
    USING dummy
) USING dummy;

SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *, t1_00850.*, t2_00850.*
    FROM ( SELECT toUInt8(0) AS dummy ) t1_00850
    INNER JOIN ( SELECT toUInt8(0) AS dummy ) t2_00850
    USING dummy
) USING dummy;

SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *, dummy
    FROM ( SELECT toUInt8(0) AS dummy ) t1_00850
    INNER JOIN ( SELECT toUInt8(0) AS dummy ) t2_00850
    USING dummy
) USING dummy;

SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *, dummy as other
    FROM ( SELECT dummy FROM remote('127.0.0.3', system.one) ) t1_00850
    GLOBAL INNER JOIN ( SELECT toUInt8(0) AS dummy ) t2_00850
    USING dummy
) USING dummy;

SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *, dummy, dummy as other
    FROM ( SELECT toUInt8(0) AS dummy ) t1_00850
    GLOBAL INNER JOIN ( SELECT dummy FROM remote('127.0.0.3', system.one) ) t2_00850
    USING dummy
) USING dummy;
