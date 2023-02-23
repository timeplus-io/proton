-- Tags: no-parallel
-- because of system.tables poisoning

DROP STREAM IF EXISTS test;
CREATE STREAM test (key uint32) Engine = Buffer(current_database(), test, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT * FROM test; -- { serverError 269 }
SELECT * FROM system.tables WHERE table = 'test' AND database = current_database() FORMAT Null; -- { serverError 269 }
DROP STREAM test;

DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
CREATE STREAM test1 (key uint32) Engine = Buffer(current_database(), test2, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
CREATE STREAM test2 (key uint32) Engine = Buffer(current_database(), test1, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT * FROM test1; -- { serverError 306 }
SELECT * FROM test2; -- { serverError 306 }
SELECT * FROM system.tables WHERE table IN ('test1', 'test2') AND database = current_database(); -- { serverError 306 }
DROP STREAM test1;
DROP STREAM test2;
