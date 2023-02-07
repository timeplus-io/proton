DROP STREAM IF EXISTS t1__fuzz_13;
DROP STREAM IF EXISTS t2__fuzz_47;

SET allow_suspicious_low_cardinality_types = 1;

CREATE STREAM t1__fuzz_13 (id nullable(int16)) ENGINE = MergeTree() ORDER BY id SETTINGS allow_nullable_key = 1;
CREATE STREAM t2__fuzz_47 (id low_cardinality(int16)) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t1__fuzz_13 VALUES (1);
INSERT INTO t2__fuzz_47 VALUES (1);

SELECT * FROM t1__fuzz_13 FULL OUTER JOIN t2__fuzz_47 ON 1 = 2;
