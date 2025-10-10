CREATE RANDOM STREAM device
(
 `sensor` string DEFAULT ['A', 'B', 'C'][(int_div(rand(), 1000000000) % 3) + 1],
 `temperature` float DEFAULT round(rand_normal(10, 0.5), 2)
)
SETTINGS eps = 1;

CREATE RANDOM STREAM timer
(
 `time` datetime64 DEFAULT now64()
)
SETTINGS eps = 1;

-- target stream
CREATE STREAM device_reader
(
 `label` string,
 `sensor` string,
 `temperature` float32
);


-- simulate get data from source to proton
CREATE MATERIALIZED VIEW mv_timer_reader INTO device_reader
AS
SELECT
 'timer' as label, '' as sensor, 0 as temperature
FROM
 timer;

CREATE MATERIALIZED VIEW mv_device_reader INTO device_reader
AS
SELECT
 'device' as label, sensor, temperature
FROM
 device;


-- monitor the stream

SELECT
  window_start, group_uniq_array(label) as unique_labels
FROM
  tumble(device_reader, 3s)
GROUP BY
  window_start;

-- send alert in case of missing data from device
SELECT
  window_start, group_uniq_array(label) as unique_labels
FROM
  tumble(device_reader, 3s)
GROUP BY
  window_start
HAVING index_of(unique_labels,'device') = 0;





