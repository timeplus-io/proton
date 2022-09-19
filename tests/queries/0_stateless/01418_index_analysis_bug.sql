DROP STREAM IF EXISTS mytable_local;

create stream mytable_local (
    created          DateTime,
    eventday         date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id);

INSERT INTO mytable_local SELECT
    to_datetime('2020-06-01 00:00:00') + toIntervalMinute(number) AS created,
    to_date(created) AS eventday,
    if((number % 100) > 50, 742522, number % 32141) AS user_id
FROM numbers(100000);

SELECT
    eventday,
    count(*)
FROM mytable_local
WHERE (toYYYYMM(eventday) = 202007) AND (user_id = 742522) AND (eventday >= '2020-07-03') AND (eventday <= '2020-07-25')
GROUP BY eventday
ORDER BY eventday;

DROP STREAM mytable_local;
DROP STREAM IF EXISTS table_float;

create stream table_float
(
    f float64,
    u uint32
)
ENGINE = MergeTree
ORDER BY (f, u);

INSERT INTO table_float VALUES (1.2, 1) (1.3, 2) (1.4, 3) (1.5, 4);

SELECT count()
FROM table_float
WHERE (to_uint64(f) = 1) AND (f >= 1.3) AND (f <= 1.4) AND (u > 0);

DROP STREAM table_float;
