SELECT proportions_ztest(10, 11, 100, 101, 0.95, 'unpooled');

DROP STREAM IF EXISTS proportions_ztest;
CREATE STREAM proportions_ztest (sx uint64, sy uint64, tx uint64, ty uint64) Engine = Memory();
INSERT INTO proportions_ztest VALUES (10, 11, 100, 101);
SELECT proportions_ztest(sx, sy, tx, ty, 0.95, 'unpooled') FROM proportions_ztest;
DROP STREAM IF EXISTS proportions_ztest;


SELECT
    NULL,
    proportions_ztest(257, 1048575, 1048575, 257, -inf, NULL),
    proportions_ztest(1024, 1025, 2, 2, 'unpooled'); -- { serverError 43 }