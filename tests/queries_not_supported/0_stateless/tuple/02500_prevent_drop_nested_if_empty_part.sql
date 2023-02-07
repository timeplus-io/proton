DROP STREAM IF EXISTS 02500_nested;

SET flatten_nested = 1;

CREATE STREAM 02500_nested(arr array(tuple(a int32, b int32))) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(arr.a, arr.b) VALUES ([1], [2]);
ALTER STREAM 02500_nested ADD COLUMN z int32;
ALTER STREAM 02500_nested DROP COLUMN arr; -- { serverError BAD_ARGUMENTS }
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(arr array(tuple(a int32, b int32)), z int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(arr.a, arr.b, z) VALUES ([1], [2], 2);
ALTER STREAM 02500_nested DROP COLUMN arr;
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(nes Nested(a int32, b int32)) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes.a, nes.b) VALUES ([1], [2]);
ALTER STREAM 02500_nested ADD COLUMN z int32;
ALTER STREAM 02500_nested DROP COLUMN nes; -- { serverError BAD_ARGUMENTS }
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(nes array(tuple(a int32, b int32)), z int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes.a, nes.b, z) VALUES ([1], [2], 2);
ALTER STREAM 02500_nested DROP COLUMN nes;
DROP STREAM 02500_nested;

SET flatten_nested = 0;

CREATE STREAM 02500_nested(arr array(tuple(a int32, b int32))) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(arr) VALUES ([(1, 2)]);
ALTER STREAM 02500_nested ADD COLUMN z int32;
ALTER STREAM 02500_nested DROP COLUMN arr; -- { serverError BAD_ARGUMENTS }
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(arr array(tuple(a int32, b int32)), z int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(arr, z) VALUES ([(1, 2)], 2);
ALTER STREAM 02500_nested DROP COLUMN arr;
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(nes Nested(a int32, b int32)) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes) VALUES ([(1, 2)]);
ALTER STREAM 02500_nested ADD COLUMN z int32;
ALTER STREAM 02500_nested DROP COLUMN nes; -- { serverError BAD_ARGUMENTS }
DROP STREAM 02500_nested;

CREATE STREAM 02500_nested(nes array(tuple(a int32, b int32)), z int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes, z) VALUES ([(1, 2)], 2);
ALTER STREAM 02500_nested DROP COLUMN nes;
DROP STREAM 02500_nested;
