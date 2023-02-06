SELECT l1_norm([1, 2, 3]);
SELECT l2_norm([3., 4., 5.]);
SELECT l2_squared_norm([3., 4., 5.]);
SELECT lp_norm([3., 4., 5.], 1.1);
SELECT linf_norm([0, 0, 2]);

-- Overflows
WITH CAST([-547274980, 1790553898, 1981517754, 1908431500, 1352428565, -573412550, -552499284, 2096941042], 'array(int32)') AS a
SELECT
    l1_norm(a),
    l2_norm(a),
    l2_squared_norm(a),
    lp_norm(a,1),
    lp_norm(a,2),
    lp_norm(a,3.14),
    linf_norm(a);

DROP STREAM IF EXISTS vec1;
DROP STREAM IF EXISTS vec1f;
DROP STREAM IF EXISTS vec1d;
CREATE STREAM vec1 (id uint64, v array(uint8)) ENGINE = Memory;
CREATE STREAM vec1f (id uint64, v array(float32)) ENGINE = Memory;
CREATE STREAM vec1d (id uint64, v array(float64)) ENGINE = Memory;
INSERT INTO vec1 VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));
INSERT INTO vec1f VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));
INSERT INTO vec1d VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));

SELECT id, l1_norm(v), l2_norm(v), l2_squared_norm(v), lp_norm(v, 2.7), linf_norm(v) FROM vec1;
SELECT id, l1_norm(materialize([5., 6.])) FROM vec1;

SELECT id, l1_norm(v), l2_norm(v), l2_squared_norm(v), lp_norm(v, 2.7), linf_norm(v) FROM vec1f;
SELECT id, l1_norm(materialize([5., 6.])) FROM vec1f;

SELECT id, l1_norm(v), l2_norm(v), l2_squared_norm(v), lp_norm(v, 2.7), linf_norm(v) FROM vec1d;
SELECT id, l1_norm(materialize([5., 6.])) FROM vec1d;

SELECT l1_norm(1, 2); -- { serverError 42 }

SELECT lp_norm([1,2]); -- { serverError 42 }
SELECT lp_norm([1,2], -3.4); -- { serverError 69 }
SELECT lp_norm([1,2], 'aa'); -- { serverError 43 }
SELECT lp_norm([1,2], [1]); -- { serverError 43 }
SELECT lp_norm([1,2], materialize(3.14)); -- { serverError 44 }

DROP STREAM vec1;
DROP STREAM vec1f;
DROP STREAM vec1d;
