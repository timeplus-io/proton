
-- simulation of iot data source
CREATE RANDOM STREAM device
(
 `sensor` string DEFAULT ['A', 'B', 'C'][(int_div(rand(), 1000000000) % 3) + 1],
 `temperature` float DEFAULT round(rand_normal(10, 0.5), 2)
)
SETTINGS eps = 1;

-- target stream
CREATE STREAM device_reader
(
 `sensor` string,
 `temperature` float32
);

-- simulate get data from source to proton
CREATE MATERIALIZED VIEW mv_device_reader INTO device_reader
AS
SELECT
 *
FROM
 device;

-- continously monitor the total observed count 
SELECT count(*) FROM device_reader EMIT PERIODIC 2s REPEAT;

-- using lag to detect the count change 
WITH accumulate_count AS (
    SELECT count(*) AS count FROM device_reader EMIT PERIODIC 2s REPEAT
)
SELECT count, lag(count) AS previous_count FROM accumulate_count;

-- alert on broken stream
WITH accumulate_count AS (
    SELECT count(*) AS count FROM device_reader EMIT PERIODIC 2s REPEAT
)
SELECT count, lag(count) AS previous_count FROM accumulate_count
WHERE count = previous_count;

-- drop the mv to simulate the broken stream
DROP VIEW mv_device_reader;
