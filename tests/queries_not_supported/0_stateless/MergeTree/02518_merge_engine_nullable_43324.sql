
DROP STREAM IF EXISTS foo;
DROP STREAM IF EXISTS foo__fuzz_0;
DROP STREAM IF EXISTS foo_merge;

CREATE STREAM foo (`Id` int32, `Val` int32) ENGINE = MergeTree ORDER BY Id;
CREATE STREAM foo__fuzz_0 (`Id` int64, `Val` nullable(int32)) ENGINE = MergeTree ORDER BY Id;

INSERT INTO foo SELECT number, number % 5 FROM numbers(10);
INSERT INTO foo__fuzz_0 SELECT number, number % 5 FROM numbers(10);

CREATE STREAM merge1 AS foo ENGINE = Merge(current_database(), '^foo');
CREATE STREAM merge2 (`Id` int32, `Val` int32) ENGINE = Merge(current_database(), '^foo');
CREATE STREAM merge3 (`Id` int32, `Val` int32) ENGINE = Merge(current_database(), '^foo__fuzz_0');

SELECT * FROM merge1 WHERE Val = 3 AND Val = 1;
SELECT * FROM merge2 WHERE Val = 3 AND Val = 1;
SELECT * FROM merge3 WHERE Val = 3 AND Val = 1;
