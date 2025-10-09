-- Tags: disabled
-- https://github.com/timeplus-io/proton-enterprise/issues/9989

DROP STREAM IF EXISTS t;

CREATE STREAM t (uid int16, name string, age nullable(int8), i int16, j int16, projection p1 (select name, age, uniq(i), count(j) group by name, age)) ENGINE=MergeTree order by uid settings index_granularity = 1;

INSERT INTO t VALUES (1231, 'John', 11, 1, 1), (6666, 'Ksenia', 1, 2, 2), (8888, 'Alice', 1, 3, 3), (6667, 'Ksenia', null, 4, 4);

-- Cannot ALTER, which breaks key column of projection.
ALTER STREAM t MODIFY COLUMN age nullable(int32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Cannot ALTER, uniq(Int16) is not compatible with uniq(Int32).
ALTER STREAM t MODIFY COLUMN i int32; -- { serverError CANNOT_CONVERT_TYPE }

SYSTEM STOP MERGES t;

SET alter_sync = 0;

-- Can ALTER, count(Int16) is compatible with count(Int32).
ALTER STREAM t MODIFY COLUMN j int32;

-- Projection query works without mutation applied.
SELECT count(j) FROM t GROUP BY name, age;

SYSTEM START MERGES t;

SET alter_sync = 1;

-- Another ALTER to wait for.
ALTER STREAM t MODIFY COLUMN j int64 SETTINGS mutations_sync = 2;

-- Projection query works with mutation applied.
SELECT count(j) FROM t GROUP BY name, age;

DROP STREAM t;
