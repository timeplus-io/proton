-- Tags: shard

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS t4;

create stream t1(x uint32, y uint32) ENGINE TinyLog;
create stream t2(x uint32, y uint32 DEFAULT x + 1) ENGINE TinyLog;
create stream t3(x uint32, y uint32 MATERIALIZED x + 1) ENGINE TinyLog;
create stream t4(x uint32, y uint32 ALIAS x + 1) ENGINE TinyLog;

INSERT INTO t1 VALUES (1, 1);
INSERT INTO t2 VALUES (1, 1);
INSERT INTO t3 VALUES (1);
INSERT INTO t4 VALUES (1);

INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t1) VALUES (2, 2);
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t2) VALUES (2, 2);
--TODO: INSERT into remote tables with MATERIALIZED columns.
--INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t3) VALUES (2);
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t4) VALUES (2);

SELECT * FROM remote('127.0.0.2', currentDatabase(), t1) ORDER BY x;

SELECT '*** With a DEFAULT column ***';
SELECT * FROM remote('127.0.0.2', currentDatabase(), t2) ORDER BY x;

SELECT '*** With a MATERIALIZED column ***';
SELECT * FROM remote('127.0.0.2', currentDatabase(), t3) ORDER BY x;
SELECT x, y FROM remote('127.0.0.2', currentDatabase(), t3) ORDER BY x;

SELECT '*** With an ALIAS column ***';
SELECT * FROM remote('127.0.0.2', currentDatabase(), t4) ORDER BY x;
SELECT x, y FROM remote('127.0.0.2', currentDatabase(), t4) ORDER BY x;

DROP STREAM t1;
DROP STREAM t2;
DROP STREAM t3;
DROP STREAM t4;
