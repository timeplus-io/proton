# Demo to visualize the sensor data from your phone

[Sensor Logger](https://github.com/tszheichoi/awesome-sensor-logger) is a free, easy-to-use, cross-platform data logger that logs readings from common motion-related sensors on smartphones. Recordings can be exported as a zipped CSV file, or streamed to Proton via HTTP API.

Demo video: https://youtu.be/vi4Yl6L4_Dw?t=1049

## Setup Guide

### Step 1: start docker compose
Install [Docker Desktop](https://docs.docker.com/desktop/) and fork this repo or download the [docker-compose.yml](docker-compose.yml) to a local folder. Start everything via `docker compose up`. The following containers will be started:
* latest version of Proton, to receive the live data and apply stream processing
* latest version of Grafana, to visualize the live data with streaming SQL
* a lightweight proxy server to convert JSON from sensor loggers to the format for Proton Ingest API

Wait for about half a minute to have all containers up running.

### Step 2: install mobile app and push data to Proton
Download Sensor Logger at www.tszheichoi.com/sensorlogger.

| Android | iOS |
|:-:|:-:|
| [<img src="https://play.google.com/intl/en_us/badges/static/images/badges/en_badge_web_generic.png" height="50">](https://play.google.com/store/apps/details?id=com.kelvin.sensorapp&pcampaignid=pcampaignidMKT-Other-global-all-co-prtnr-py-PartBadge-Mar2515-1) | [<img src="https://developer.apple.com/app-store/marketing/guidelines/images/badge-example-preferred_2x.png" height="50">](https://apps.apple.com/app/id1531582925) |

Open the app, and go to the settings page, by clicking the button at the bottom.
1. Click the `Data Streaming` menu.
2. Turn on `Enable HTTP Push`.
3. Get your IP for your server (on Mac, you can hold Option key and click the WiFi icon to get the IP address), and set the `Push URL` as `http://<ip>:8000`
4. Optionally, you can turn on `Skip Writing`.

### Step 3: view the live dashboard in Grafana

In your laptop/server, access the Grafana UI via `http://localhost:3000` and open the `Phone Sensor` dashboard.
