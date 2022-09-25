-- Tags: deadlock

DROP STREAM IF EXISTS t;
create stream t (x uint8) ;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t SELECT * FROM t;
SELECT count() FROM t;

DROP STREAM t;


create stream t (x uint8)  ;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t SELECT * FROM t;
SELECT count() FROM t;

DROP STREAM t;


create stream t (x uint8) ENGINE = StripeLog;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t SELECT * FROM t;
SELECT count() FROM t;

DROP STREAM t;
