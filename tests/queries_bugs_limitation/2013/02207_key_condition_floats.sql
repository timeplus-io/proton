DROP STREAM IF EXISTS t_key_condition_float;

CREATE STREAM t_key_condition_float (a float32)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_key_condition_float VALUES (0.1), (0.2);

SELECT count() FROM t_key_condition_float WHERE a > 0;
SELECT count() FROM t_key_condition_float WHERE a > 0.0;
SELECT count() FROM t_key_condition_float WHERE a > 0::float32;
SELECT count() FROM t_key_condition_float WHERE a > 0::float64;

DROP STREAM t_key_condition_float;

CREATE STREAM t_key_condition_float (a float64)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_key_condition_float VALUES (0.1), (0.2);

SELECT count() FROM t_key_condition_float WHERE a > 0;
SELECT count() FROM t_key_condition_float WHERE a > 0.0;
SELECT count() FROM t_key_condition_float WHERE a > 0::float32;
SELECT count() FROM t_key_condition_float WHERE a > 0::float64;

DROP STREAM t_key_condition_float;

CREATE STREAM t_key_condition_float (a uint64)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_key_condition_float VALUES (1), (2);

SELECT count() FROM t_key_condition_float WHERE a > 1.5;

DROP STREAM t_key_condition_float;
