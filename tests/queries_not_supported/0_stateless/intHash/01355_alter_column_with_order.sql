-- Tags: no-parallel

DROP STREAM IF EXISTS alter_test;

create stream alter_test (CounterID uint32, StartDate date, UserID uint32, VisitID uint32, NestedColumn nested(A uint8, S string), ToDrop uint32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);

ALTER STREAM alter_test ADD COLUMN Added1 uint32 FIRST;

ALTER STREAM alter_test ADD COLUMN Added2 uint32 AFTER NestedColumn;

ALTER STREAM alter_test ADD COLUMN Added3 uint32 AFTER ToDrop;

DESC alter_test;
DETACH TABLE alter_test;
ATTACH TABLE alter_test;
DESC alter_test;

ALTER STREAM alter_test MODIFY COLUMN Added2 uint32 FIRST;

ALTER STREAM alter_test MODIFY COLUMN Added3 uint32 AFTER CounterID;

DESC alter_test;
DETACH TABLE alter_test;
ATTACH TABLE alter_test;
DESC alter_test;

DROP STREAM IF EXISTS alter_test;
