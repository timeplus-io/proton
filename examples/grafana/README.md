# Demo for Grafana plugin with carsharing data



This docker compose file demonstrates how to use Grafana to connect to Proton and visualize the data.

## Start the example

Simply run `docker compose up` in this folder. Two docker containers in the stack:

1. ghcr.io/timeplus-io/proton:latest, as the streaming database. Port 8463 is exposed so that Grafana can connect to it.
2. timeplus/cardemo:latest, as the data generator

TODO: we will update the compose file later to include a preconfigured Grafana.

## Use the Grafana plugin for Proton

Install the timeplus-proton-datasource in your Grafana plugin folder, such as
- /var/lib/grafana/plugins (for Linux)
- /usr/local/var/lib/grafana/plugins (for Homebrew on Mac)

Unzip the file and restart Grafana. 

Before the plugin is approved by Grafana, you need to set your Grafana running in development mode via changing /usr/local/etc/grafana/grafana.ini, setting `app_mode = development`

In the navigation menu, choose Connections -> Add new connection.
Search for Proton and accept the default settings (localhost:8463 as proton connection)

Create a new dashboard or explore data with this Proton data source.

By default, the "Streaming Query" toggle is off. If your SQL is a streaming SQL, make sure to turn it on to leverage Grafana's live chart to show the new results.

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

