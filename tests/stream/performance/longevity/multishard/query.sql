WITH get_datapoints_cnt_per_lpn AS
                  (
                    SELECT
                      lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt
                    FROM
                 mv_truck_track_$
                    PARTITION BY
                      lpn
                    EMIT PERIODIC 5s
                    SETTINGS
                      seek_to = '-2d', max_threads = 10
                  ), get_last_count AS
                  (
                    SELECT
                      *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt
                    FROM
                 get_datapoints_cnt_per_lpn settings max_threads = 10
                  ), get_per_30s AS
                  (
                    SELECT
                      *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s
                    FROM
                 get_last_count
                  ), get_idle AS
                  (
                    SELECT
                      lpn, window_start AS windowStart, window_end AS windowEnd, latest(last_spd1) AS last_spd, latest(lat1) AS lat, latest(lon1) AS lon, sum(cnt_per_30s) AS cnt_per_1m, sum(sum_spd_per_30s) AS sum_spd_per_1m, if(cnt_per_1m = 0, 'OFF', if(sum_spd_per_1m = 0, 'IDLE', 'RUNNING')) AS status
                    FROM
                 hop(get_per_30s, now64(3, 'UTC'), 30s, 1m)
                    GROUP BY
                      window_start, window_end, lpn
                  ), get_off AS
                  (
                    SELECT
                      *, lag(status, 1, 'OFF') OVER (PARTITION BY lpn) AS lag_status
                    FROM
                 get_idle
                  )
                SELECT
                  lpn, windowStart, windowEnd, windowEnd AS _tp_time, if(status = 'OFF', if(lag_status = 'OFF', 'OFF', 'IDLE'), if(status = 'IDLE', 'IDLE', 'RUNNING')) AS state, if(state = 'OFF', 0, sum_spd_per_1m / cnt_per_1m) AS avg_speed, if(state = 'OFF', 0, last_spd) AS last_speed, lat, lon
                FROM
                 get_off


SELECT lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt FROM `mv_truck_track_$` PARTITION BY lpn EMIT PERIODIC 5s SETTINGS seek_to = '-2d', max_threads = 10


WITH get_datapoints_cnt_per_lpn AS (SELECT lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt FROM mv_truck_track_$ PARTITION BY lpn EMIT PERIODIC 5s SETTINGS seek_to = '-2d', max_threads = 10) SELECT *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt FROM get_datapoints_cnt_per_lpn settings max_threads = 10


WITH get_datapoints_cnt_per_lpn AS (SELECT lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt FROM mv_truck_track_$ PARTITION BY lpn EMIT PERIODIC 5s SETTINGS seek_to = '-2d', max_threads = 10), get_last_count AS (SELECT *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt FROM get_datapoints_cnt_per_lpn settings max_threads = 10)SELECT *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s FROM get_last_count


WITH get_datapoints_cnt_per_lpn AS(SELECT lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt FROM mv_truck_track_$ PARTITION BY lpn EMIT PERIODIC 5s SETTINGS max_threads = 10), get_last_count AS(SELECT *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt FROM get_datapoints_cnt_per_lpn settings max_threads = 10), get_per_30s AS(SELECT *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s FROM get_last_count) SELECT lpn, window_start AS windowStart, window_end AS windowEnd, latest(last_spd1) AS last_spd, latest(lat1) AS lat, latest(lon1) AS lon, sum(cnt_per_30s) AS cnt_per_1m, sum(sum_spd_per_30s) AS sum_spd_per_1m, if(cnt_per_1m = 0, 'OFF', if(sum_spd_per_1m = 0, 'IDLE', 'RUNNING')) AS status FROM hop(get_per_30s, now64(3, 'UTC'), 1s, 5s) partition by lpn GROUP BY window_start, window_end



WITH get_datapoints_cnt_per_lpn AS
                  (
                    SELECT
                      lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt
                    FROM
                 mv_truck_track_$
                    PARTITION BY
                      lpn
                    EMIT PERIODIC 5s
                    SETTINGS
                      seek_to = '-2d', max_threads = 10
                  ), get_last_count AS
                  (
                    SELECT
                      *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt
                    FROM
                 get_datapoints_cnt_per_lpn settings max_threads = 10
                  ), get_per_30s AS
                  (
                    SELECT
                      *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s
                    FROM
                 get_last_count
                  )
                    SELECT
                      lpn, window_start AS windowStart, window_end AS windowEnd, latest(last_spd1) AS last_spd, latest(lat1) AS lat, latest(lon1) AS lon, sum(cnt_per_30s) AS cnt_per_1m, sum(sum_spd_per_30s) AS sum_spd_per_1m, if(cnt_per_1m = 0, 'OFF', if(sum_spd_per_1m = 0, 'IDLE', 'RUNNING')) AS status
                    FROM
                 hop(get_per_30s, now64(3, 'UTC'), 30s, 1m)
                    GROUP BY
                      window_start, window_end, lpn
                  







WITH get_datapoints_cnt_per_lpn AS
                  (
                    SELECT
                      lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt
                    FROM
                 mv_truck_track_$
                    PARTITION BY
                      lpn
                    EMIT PERIODIC 5s
                    SETTINGS
                      seek_to = '-2d', max_threads = 10
                  ), get_last_count AS
                  (
                    SELECT
                      *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt
                    FROM
                 get_datapoints_cnt_per_lpn settings max_threads = 10
                  ), get_per_30s AS
                  (
                    SELECT
                      *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s
                    FROM
                 get_last_count
                  )
                    SELECT
                      lpn, window_start AS windowStart, window_end AS windowEnd, latest(last_spd1) AS last_spd, latest(lat1) AS lat, latest(lon1) AS lon, sum(cnt_per_30s) AS cnt_per_1m, sum(sum_spd_per_30s) AS sum_spd_per_1m, if(cnt_per_1m = 0, 'OFF', if(sum_spd_per_1m = 0, 'IDLE', 'RUNNING')) AS status
                    FROM
                 hop(get_per_30s, now64(3, 'UTC'), 5s, 10s)
                    GROUP BY
                      window_start, window_end, lpn

WITH get_datapoints_cnt_per_lpn AS
                  (
                    SELECT
                      lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt
                    FROM
                 mv_truck_track_$
                    PARTITION BY
                      lpn
                    EMIT PERIODIC 5s
                    SETTINGS
                        max_threads = 10
                  ), get_last_count AS
                  (
                    SELECT
                      *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt
                    FROM
                 get_datapoints_cnt_per_lpn settings max_threads = 10
                  ), get_per_30s AS
                  (
                    SELECT
                      *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s
                    FROM
                 get_last_count
                  )
                    SELECT
                      lpn, window_start AS windowStart, window_end AS windowEnd, latest(last_spd1) AS last_spd, latest(lat1) AS lat, latest(lon1) AS lon, sum(cnt_per_30s) AS cnt_per_1m, sum(sum_spd_per_30s) AS sum_spd_per_1m, if(cnt_per_1m = 0, 'OFF', if(sum_spd_per_1m = 0, 'IDLE', 'RUNNING')) AS status
                    FROM
                 hop(get_per_30s, now64(3, 'UTC'), 1s, 5s)
                    partition by lpn
                    GROUP BY
                      window_start, window_end, lpn


WITH get_datapoints_cnt_per_lpn AS
                  (
                    SELECT
                      lpn, latest(spd) AS last_spd1, latest(wgs84Lat) AS lat1, latest(wgs84Lon) AS lon1, sum(spd) AS sum_spd, count(*) AS cnt
                    FROM
                 mv_truck_track_$
                    PARTITION BY
                      lpn
                    EMIT PERIODIC 5s
                    SETTINGS
                      max_threads = 10
                  ), get_last_count AS
                  (
                    SELECT
                      *, lag(sum_spd) OVER (PARTITION BY lpn) AS last_sum_spd, lag(cnt) OVER (PARTITION BY lpn) AS last_cnt
                    FROM
                 get_datapoints_cnt_per_lpn settings max_threads = 10
                  )
                    SELECT
                      *, cnt - last_cnt AS cnt_per_30s, sum_spd - last_sum_spd AS sum_spd_per_30s
                    FROM
                 get_last_count

CREATE STREAM if not exists ttp_multi_shard_$(`lpn` string, `vno` string, `drc` string, `drcCode` int32, `wgs84Lat` float32, `wgs84Lon` float32, `gcj02Lat` float32, `gcj02Lon` float32, `province` nullable(string), `city` nullable(string), `country` nullable(string), `spd` float32, `mil` float32, `time` string, `adr` string, perf_event_time datetime64(3) default now64(3,'Asia/Shanghai'), _perf_row_id string, _perf_ingest_time string) engine=Stream(16,1,lpn) TTL to_datetime(_tp_time) + INTERVAL 24 HOUR SETTINGS logstore_retention_bytes = '21474836480', logstore_retention_ms = '00000'                                                                          