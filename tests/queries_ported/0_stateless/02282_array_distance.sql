SELECT l1_distance([0, 0, 0], [1, 2, 3]);
SELECT l2_distance([1, 2, 3], [0, 0, 0]);
SELECT l2_squared_distance([1, 2, 3], [0, 0, 0]);
SELECT lp_distance([1, 2, 3], [0, 0, 0], 3.5);
SELECT linf_distance([1, 2, 3], [0, 0, 0]);
SELECT cosine_distance([1, 2, 3], [3, 5, 7]);

SELECT l2_distance([1, 2, 3], NULL);
SELECT l2_squared_distance([1, 2, 3], NULL);
SELECT cosine_distance([1, 2, 3], [0, 0, 0]);

-- Overflows
WITH CAST([-547274980, 1790553898, 1981517754, 1908431500, 1352428565, -573412550, -552499284, 2096941042], 'array(int32)') AS a
SELECT
    l1_distance(a,a),
    l2_distance(a,a),
    l2_squared_distance(a,a),
    linf_distance(a,a),
    cosine_distance(a, a);

DROP STREAM IF EXISTS vec1;
DROP STREAM IF EXISTS vec2;
DROP STREAM IF EXISTS vec2f;
DROP STREAM IF EXISTS vec2d;
CREATE STREAM vec1 (id uint64, v array(uint8)) ENGINE = Memory;
CREATE STREAM vec2 (id uint64, v array(int64)) ENGINE = Memory;
CREATE STREAM vec2f (id uint64, v array(float32)) ENGINE = Memory;
CREATE STREAM vec2d (id uint64, v array(float64)) ENGINE = Memory;

INSERT INTO vec1 VALUES (1, [3, 4, 5]), (2, [2, 4, 8]), (3, [7, 7, 7]);
SELECT l1_distance(v, [0, 0, 0]) FROM vec1;
SELECT l2_distance(v, [0, 0, 0]) FROM vec1;
SELECT l2_squared_distance(v, [0, 0, 0]) FROM vec1;
SELECT lp_distance(v, [0, 0, 0], 3.14) FROM vec1;
SELECT linf_distance([5, 4, 3], v) FROM vec1;
SELECT cosine_distance([3, 2, 1], v) FROM vec1;
SELECT linf_distance(v, materialize([0, -2, 0])) FROM vec1;
SELECT cosine_distance(v, materialize([1., 1., 1.])) FROM vec1;

INSERT INTO vec2 VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    l1_distance(v1.v, v2.v),
    linf_distance(v1.v, v2.v),
    lp_distance(v1.v, v2.v, 3.1),
    l2_distance(v1.v, v2.v),
    l2_squared_distance(v1.v, v2.v),
    cosine_distance(v1.v, v2.v)
FROM vec2 v1, vec2 v2
WHERE length(v1.v) == length(v2.v);

INSERT INTO vec2f VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    l1_distance(v1.v, v2.v),
    linf_distance(v1.v, v2.v),
    lp_distance(v1.v, v2.v, 3),
    l2_distance(v1.v, v2.v),
    l2_squared_distance(v1.v, v2.v),
    cosine_distance(v1.v, v2.v)
FROM vec2f v1, vec2f v2
WHERE length(v1.v) == length(v2.v);

INSERT INTO vec2d VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    l1_distance(v1.v, v2.v),
    linf_distance(v1.v, v2.v),
    lp_distance(v1.v, v2.v, 3),
    l2_distance(v1.v, v2.v),
    l2_squared_distance(v1.v, v2.v),
    cosine_distance(v1.v, v2.v)
FROM vec2d v1, vec2d v2
WHERE length(v1.v) == length(v2.v);

SELECT
    v1.id,
    v2.id,
    l1_distance(v1.v, v2.v),
    linf_distance(v1.v, v2.v),
    lp_distance(v1.v, v2.v, 3),
    l2_distance(v1.v, v2.v),
    l2_squared_distance(v1.v, v2.v),
    cosine_distance(v1.v, v2.v)
FROM vec2f v1, vec2d v2
WHERE length(v1.v) == length(v2.v);

SELECT l1_distance([0, 0], [1]); -- { serverError 190 }
SELECT l2_distance([1, 2], (3,4)); -- { serverError 43 }
SELECT l2_squared_distance([1, 2], (3,4)); -- { serverError 43 }
SELECT lp_distance([1, 2], [3,4]); -- { serverError 42 }
SELECT lp_distance([1, 2], [3,4], -1.); -- { serverError 69 }
SELECT lp_distance([1, 2], [3,4], 'aaa'); -- { serverError 43 }
SELECT lp_distance([1, 2], [3,4], materialize(2.7)); -- { serverError 44 }

DROP STREAM vec1;
DROP STREAM vec2;
DROP STREAM vec2f;
DROP STREAM vec2d;
