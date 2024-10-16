# Demo for Grafana plugin with carsharing data

This docker compose file demonstrates how to use Grafana to connect to Proton and visualize the data.

A YouTube video tutorial is available for visual learners: https://www.youtube.com/watch?v=cBRl1k9qWZc

## Start the example

Simply run `docker compose up` in this folder. Three docker containers in the stack:

1. d.timeplus.com/timeplus-io/proton:latest, as the streaming SQL engine. Port 8463 and 3218 are exposed so that Grafana can connect to it.
2. timeplus/cardemo:latest, as the data generator
3. grafana/grafana:latest, with pre-configured Proton dashboard and a live dashboard

When all containers are up running, access http://localhost:3000 and open the `Carsharing Demo Dashboard` dashboard.

## Sample queries

Please check https://docs.timeplus.com/usecases for more sample queries.

```sql
-- List live data
SELECT * FROM car_live_data;

-- Filter data
SELECT time,cid,gas_percent FROM car_live_data WHERE gas_percent < 25;

-- Downsampling
SELECT window_start,cid, avg(gas_percent) AS avg_gas_percent,avg(speed_kmh) AS avg_speed FROM
tumble(car_live_data,1m) GROUP BY window_start, cid;

-- Create materlized view
CREATE MATERIALIZED VIEW car_live_data_1min as
SELECT window_start AS time,cid, avg(gas_percent) AS avg_gas,avg(speed_kmh) AS avg_speed
FROM tumble(car_live_data,1m) GROUP BY window_start, cid;
SELECT * FROM car_live_data_1min;

-- Top K
SELECT window_start,top_k(cid,3) AS popular_cars FROM tumble(bookings,1h) GROUP BY window_start;

-- JOIN
SELECT avg(gap) FROM
( SELECT
    date_diff('second', bookings.booking_time, trips.start_time) AS gap
  FROM bookings
  INNER JOIN trips ON (bookings.bid = trips.bid)
     AND date_diff_within(2m, bookings.booking_time, trips.start_time)
) WHERE _tp_time >= now()-1d;

```
