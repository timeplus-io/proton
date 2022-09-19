-- Tags: bug, #1305
SET query_mode = 'table';
DROP STREAM IF EXISTS alter_test;

create stream alter_test (CounterID uint32, StartDate date, UserID uint32, VisitID uint32, NestedColumn nested(A uint8, S string), ToDrop uint32);

INSERT INTO alter_test(CounterID, StartDate,UserID,VisitID,NestedColumn,ToDrop ) VALUES (1, '2014-01-01', 2, 3, [1,2,3], ['a','b','c'], 4);
SELECT sleep(3);

ALTER STREAM alter_test ADD COLUMN Added0 uint32;
ALTER STREAM alter_test ADD COLUMN Added2 uint32;
ALTER STREAM alter_test ADD COLUMN Added1 uint32 AFTER Added0;

ALTER STREAM alter_test ADD COLUMN AddedNested1 nested(A uint32, B uint64) AFTER Added2;
ALTER STREAM alter_test ADD COLUMN AddedNested1.C array(string) AFTER AddedNested1.B;
ALTER STREAM alter_test ADD COLUMN AddedNested2 nested(A uint32, B uint64) AFTER AddedNested1;

DESC STREAM alter_test;

ALTER STREAM alter_test DROP COLUMN ToDrop;

ALTER STREAM alter_test MODIFY COLUMN Added0 string;

ALTER STREAM alter_test DROP COLUMN NestedColumn.A;
ALTER STREAM alter_test DROP COLUMN NestedColumn.S;

ALTER STREAM alter_test DROP COLUMN AddedNested1.B;

ALTER STREAM alter_test ADD COLUMN IF NOT EXISTS Added0 uint32;
ALTER STREAM alter_test ADD COLUMN IF NOT EXISTS AddedNested1 nested(A uint32, B uint64);
ALTER STREAM alter_test ADD COLUMN IF NOT EXISTS AddedNested1.C array(string);
ALTER STREAM alter_test MODIFY COLUMN IF EXISTS ToDrop uint64;
ALTER STREAM alter_test DROP COLUMN IF EXISTS ToDrop;
ALTER STREAM alter_test COMMENT COLUMN IF EXISTS ToDrop 'new comment';

DESC STREAM alter_test;

SELECT * FROM alter_test;

DROP STREAM alter_test;
