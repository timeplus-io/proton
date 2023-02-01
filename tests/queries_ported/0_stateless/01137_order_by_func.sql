DROP STREAM IF EXISTS pk_func;
CREATE STREAM pk_func(d DateTime, ui uint32) ENGINE = MergeTree ORDER BY to_date(d);

INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-06 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-07 01:00:00', number FROM numbers(1000000);

SELECT * FROM pk_func ORDER BY to_date(d), ui LIMIT 5;

DROP STREAM pk_func;

DROP STREAM IF EXISTS nORX;
CREATE STREAM nORX (`A` int64, `B` int64, `V` int64) ENGINE = MergeTree ORDER BY (A, negate(B));
INSERT INTO nORX SELECT 111, number, number FROM numbers(10000000);

SELECT *
FROM nORX
WHERE B >= 1000
ORDER BY
    A ASC,
    -B ASC
LIMIT 3
SETTINGS max_threads = 1;

DROP STREAM nORX;
