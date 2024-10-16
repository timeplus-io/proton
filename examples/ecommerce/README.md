# Demo for ecommerce shop use cases

This docker compose file demonstrates how to read/write data in Kafka/Redpanda with External Streams.

For more details, please check https://docs.timeplus.com/proton-kafka#tutorial

## Start the example

Simply run `docker compose up` in this folder. Four docker containers in the stack:
1. d.timeplus.com/timeplus-io/proton:latest, as the streaming database.
2. quay.io/cloudhut/owl-shop:latest, as the data generator. [Owl Shop](https://github.com/cloudhut/owl-shop) is an imaginary ecommerce shop that simulates microservices exchanging data via Apache Kafka.
3. docker.redpanda.com/redpandadata/redpanda, as the Kafka compatiable streaming message bus
4. docker.redpanda.com/redpandadata/console, as the web UI to explore data in Kafka/Redpanda

## Run SQL

You can use `docker exec -it <name> proton-client` to run the SQL client in Proton container. Or use the Docker Desktop UI to choose the container, choose "Exec" tab and type `proton-client` to start the SQL client.

Run the following commands:
```sql
-- Create externarl stream to read data from Kafka/Redpanda
CREATE EXTERNAL STREAM frontend_events(raw string)
SETTINGS type='kafka',
         brokers='redpanda:9092',
         topic='owlshop-frontend-events';

-- Scan incoming events
select * from frontend_events;

-- Get live count
select count() from frontend_events;

-- Filter events by JSON attributes
select _tp_time, raw:ipAddress, raw:requestedUrl from frontend_events where raw:method='POST';

-- Show a live ASCII bar chart
select raw:method, count() as cnt, bar(cnt, 0, 40,5) as bar from frontend_events
group by raw:method order by cnt desc limit 5 by emit_version();
```
