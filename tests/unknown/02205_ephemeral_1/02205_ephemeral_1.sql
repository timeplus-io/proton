DROP STREAM IF EXISTS t_ephemeral_02205_1;

CREATE STREAM t_ephemeral_02205_1 (x uint32 DEFAULT y, y uint32 EPHEMERAL 17, z uint32 DEFAULT 5) ENGINE = Memory;

DESCRIBE t_ephemeral_02205_1;

# Test INSERT without columns list - should participate only ordinary columns (x, z)
INSERT INTO t_ephemeral_02205_1 VALUES (1, 2);
# SELECT * should only return ordinary columns (x, z) - ephemeral is not stored in the stream
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

INSERT INTO t_ephemeral_02205_1 VALUES (DEFAULT, 2);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT using ephemerals default
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, DEFAULT);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT using explicit ephemerals value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, 7);
SELECT * FROM t_ephemeral_02205_1;

# Test ALTER STREAM DELETE
ALTER STREAM t_ephemeral_02205_1 DELETE WHERE x = 7;
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT into column, defaulted to ephemeral, but explicitly provided with value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (21, 7);
SELECT * FROM t_ephemeral_02205_1;


DROP STREAM IF EXISTS t_ephemeral_02205_1;

# Test without default
CREATE STREAM t_ephemeral_02205_1 (x uint32 DEFAULT y, y uint32 EPHEMERAL, z uint32 DEFAULT 5) ENGINE = Memory;

DESCRIBE t_ephemeral_02205_1;

# Test INSERT without columns list - should participate only ordinary columns (x, z)
INSERT INTO t_ephemeral_02205_1 VALUES (1, 2);
# SELECT * should only return ordinary columns (x, z) - ephemeral is not stored in the stream
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

INSERT INTO t_ephemeral_02205_1 VALUES (DEFAULT, 2);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT using ephemerals default
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, DEFAULT);
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT using explicit ephemerals value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (DEFAULT, 7);
SELECT * FROM t_ephemeral_02205_1;

# Test ALTER STREAM DELETE
ALTER STREAM t_ephemeral_02205_1 DELETE WHERE x = 7;
SELECT * FROM t_ephemeral_02205_1;

TRUNCATE STREAM t_ephemeral_02205_1;

# Test INSERT into column, defaulted to ephemeral, but explicitly provided with value
INSERT INTO t_ephemeral_02205_1 (x, y) VALUES (21, 7);
SELECT * FROM t_ephemeral_02205_1;

DROP STREAM IF EXISTS t_ephemeral_02205_1;

