-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS test_00800;

USE test_00800;

DROP STREAM IF EXISTS join_any_inner;
DROP STREAM IF EXISTS join_any_left;
DROP STREAM IF EXISTS join_any_left_null;
DROP STREAM IF EXISTS join_all_inner;
DROP STREAM IF EXISTS join_all_left;
DROP STREAM IF EXISTS join_string_key;

CREATE STREAM join_any_inner (s string, x array(uint8), k uint64) ENGINE = Join(ANY, INNER, k);
CREATE STREAM join_any_left (s string, x array(uint8), k uint64) ENGINE = Join(ANY, LEFT, k);
CREATE STREAM join_all_inner (s string, x array(uint8), k uint64) ENGINE = Join(ALL, INNER, k);
CREATE STREAM join_all_left (s string, x array(uint8), k uint64) ENGINE = Join(ALL, LEFT, k);

INSERT INTO join_any_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_any_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_all_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_all_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);

-- read from StorageJoin

SELECT '--------read--------';
SELECT * from join_any_inner ORDER BY k;
SELECT * from join_any_left ORDER BY k;
SELECT * from join_all_inner ORDER BY k;
SELECT * from join_all_left ORDER BY k;

-- create StorageJoin streams with customized settings

CREATE STREAM join_any_left_null (s string, k uint64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO join_any_left_null VALUES ('abc', 1), ('def', 2);

-- join_get
SELECT '--------join_get--------';
SELECT join_get('join_any_left', 's', number) FROM numbers(3);
SELECT '';
SELECT join_get('join_any_left_null', 's', number) FROM numbers(3);
SELECT '';

-- Using identifier as the first argument

SELECT join_get(join_any_left, 's', number) FROM numbers(3);
SELECT '';
SELECT join_get(join_any_left_null, 's', number) FROM numbers(3);
SELECT '';

CREATE STREAM join_string_key (s string, x array(uint8), k uint64) ENGINE = Join(ANY, LEFT, s);
INSERT INTO join_string_key VALUES ('abc', [0], 1), ('def', [1, 2], 2);
SELECT join_get('join_string_key', 'x', 'abc'), join_get('join_string_key', 'k', 'abc');

USE default;

DROP STREAM test_00800.join_any_inner;
DROP STREAM test_00800.join_any_left;
DROP STREAM test_00800.join_any_left_null;
DROP STREAM test_00800.join_all_inner;
DROP STREAM test_00800.join_all_left;
DROP STREAM test_00800.join_string_key;

-- test provided by Alexander Zaitsev
DROP STREAM IF EXISTS test_00800.join_test;
CREATE STREAM test_00800.join_test (a uint8, b uint8) Engine = Join(ANY, LEFT, a);

USE test_00800;
select join_get('join_test', 'b', 1);

USE system;
SELECT join_get('test_00800.join_test', 'b', 1);

USE default;
DROP STREAM test_00800.join_test;

DROP DATABASE test_00800;
