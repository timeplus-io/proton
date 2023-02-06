-- Tags: no-fasttest

DROP STREAM IF EXISTS stream1;

CREATE STREAM stream1 (resolution uint8) ENGINE = Memory;

INSERT INTO stream1 VALUES(0);
INSERT INTO stream1 VALUES(1);
INSERT INTO stream1 VALUES(2);
INSERT INTO stream1 VALUES(3);
INSERT INTO stream1 VALUES(4);
INSERT INTO stream1 VALUES(5);
INSERT INTO stream1 VALUES(6);
INSERT INTO stream1 VALUES(7);
INSERT INTO stream1 VALUES(8);
INSERT INTO stream1 VALUES(9);
INSERT INTO stream1 VALUES(10);
INSERT INTO stream1 VALUES(11);
INSERT INTO stream1 VALUES(12);
INSERT INTO stream1 VALUES(13);
INSERT INTO stream1 VALUES(14);
INSERT INTO stream1 VALUES(15);


SELECT h3GetPentagonIndexes(resolution) AS indexes from stream1 order by indexes;
SELECT h3GetPentagonIndexes(20) AS indexes; -- { serverError 69 }

DROP STREAM stream1;

-- tests for const cols
SELECT '-- test for const cols';
SELECT h3GetPentagonIndexes(array_join([0,1,2,3,4,5,6,7,8,9,10]));
