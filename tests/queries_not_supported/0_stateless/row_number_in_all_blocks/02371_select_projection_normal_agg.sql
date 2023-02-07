DROP STREAM IF EXISTS video_log;

CREATE STREAM video_log
(
    `datetime` DateTime,
    `user_id` uint64,
    `device_id` uint64,
    `domain` low_cardinality(string),
    `bytes` uint64,
    `duration` uint64
)
ENGINE = MergeTree
PARTITION BY to_date(datetime)
ORDER BY (user_id, device_id);

DROP STREAM IF EXISTS rng;

CREATE STREAM rng
(
    `user_id_raw` uint64,
    `device_id_raw` uint64,
    `domain_raw` uint64,
    `bytes_raw` uint64,
    `duration_raw` uint64
)
ENGINE = GenerateRandom(1024);

INSERT INTO video_log SELECT
  to_unix_timestamp('2022-07-22 01:00:00')
  + (row_number_in_all_blocks() / 20000),
  user_id_raw % 100000000 AS user_id,
  device_id_raw % 200000000 AS device_id,
  domain_raw % 100,
  (bytes_raw % 1024) + 128,
  (duration_raw % 300) + 100
FROM rng
LIMIT 1728000;

INSERT INTO video_log SELECT
  to_unix_timestamp('2022-07-22 01:00:00')
  + (row_number_in_all_blocks() / 20000),
  user_id_raw % 100000000 AS user_id,
  100 AS device_id,
  domain_raw % 100,
  (bytes_raw % 1024) + 128,
  (duration_raw % 300) + 100
FROM rng
LIMIT 10;

DROP STREAM IF EXISTS video_log_result;

CREATE STREAM video_log_result
(
    `hour` DateTime,
    `sum_bytes` uint64,
    `avg_duration` float64
)
ENGINE = MergeTree
PARTITION BY to_date(hour)
ORDER BY sum_bytes;

INSERT INTO video_log_result SELECT
    to_start_of_hour(datetime) as hour,
    sum(bytes),
    avg(duration)
FROM video_log
WHERE (to_date(hour) = '2022-07-22') AND (device_id = '100') --(device_id = '100') Make sure it's not good and doesn't go into prewhere.
GROUP BY hour;


ALTER STREAM video_log ADD PROJECTION p_norm
(
    SELECT
        datetime,
        device_id,
        bytes,
        duration
    ORDER BY device_id
);

ALTER STREAM video_log MATERIALIZE PROJECTION p_norm settings mutations_sync=1;

ALTER STREAM video_log ADD PROJECTION p_agg
(
    SELECT
        to_start_of_hour(datetime) as hour,
        domain,
        sum(bytes),
        avg(duration)
    GROUP BY
        hour,
        domain
);

ALTER STREAM video_log MATERIALIZE PROJECTION p_agg settings mutations_sync=1;

SELECT
    equals(sum_bytes1, sum_bytes2),
    equals(avg_duration1, avg_duration2)
FROM
(
    SELECT
        to_start_of_hour(datetime) as hour,
        sum(bytes) AS sum_bytes1,
        avg(duration) AS avg_duration1
    FROM video_log
    WHERE (to_date(hour) = '2022-07-22') AND (device_id = '100') --(device_id = '100') Make sure it's not good and doesn't go into prewhere.
    GROUP BY hour
)
LEFT JOIN
(
    SELECT
        `hour`,
        `sum_bytes` AS sum_bytes2,
        `avg_duration` AS avg_duration2
    FROM video_log_result
)
USING (hour) settings joined_subquery_requires_alias=0;

DROP STREAM IF EXISTS video_log;

DROP STREAM IF EXISTS rng;

DROP STREAM IF EXISTS video_log_result;
