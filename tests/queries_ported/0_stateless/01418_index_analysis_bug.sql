DROP STREAM IF EXISTS mytable_local;

CREATE STREAM mytable_local (
    created          DateTime,
    eventday         Date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(eventday)
ORDER BY (eventday, user_id);

INSERT INTO mytable_local SELECT
    to_datetime('2020-06-01 00:00:00') + to_interval_minute(number) AS created,
    to_date(created) AS eventday,
    if((number % 100) > 50, 742522, number % 32141) AS user_id
FROM numbers(100000);

SELECT
    eventday,
    count(*)
FROM mytable_local
WHERE (to_YYYYMM(eventday) = 202007) AND (user_id = 742522) AND (eventday >= '2020-07-03') AND (eventday <= '2020-07-25')
GROUP BY eventday
ORDER BY eventday;

DROP STREAM mytable_local;
DROP STREAM IF EXISTS stream_float;

CREATE STREAM stream_float
(
    f float64,
    u uint32
)
ENGINE = MergeTree
ORDER BY (f, u);

INSERT INTO stream_float VALUES (1.2, 1) (1.3, 2) (1.4, 3) (1.5, 4);

SELECT count()
FROM stream_float
WHERE (to_uint64(f) = 1) AND (f >= 1.3) AND (f <= 1.4) AND (u > 0);

DROP STREAM stream_float;
