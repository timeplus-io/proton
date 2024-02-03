# Demo for ClickHouse External Table

This docker compose file demonstrates how to read from ClickHouse or write to ClickHouse with the new [External Table](https://docs.timeplus.com/proton-clickhouse-external-table) feature.

A YouTube video tutorial is available for visual learners: TBD

## Start the example

Simply run `docker compose up` in this folder. Three docker containers in the stack:

1. ghcr.io/timeplus-io/proton:latest, as the streaming SQL engine. 
2. clickhouse/clickhouse-server:latest
3. quay.io/cloudhut/owl-shop:latest, as the data generator. [Owl Shop](https://github.com/cloudhut/owl-shop) is an imaginary ecommerce shop that simulates microservices exchanging data via Apache Kafka.
4. docker.redpanda.com/redpandadata/redpanda, as the Kafka compatiable streaming message bus
5. docker.redpanda.com/redpandadata/console, as the web UI to explore data in Kafka/Redpanda

When all containers are up running, a few topics will be created in Redpanda with live demo.

## Read data from Redpanda, apply ETL and write to ClickHouse
Open the `proton client` in the proton container. Run the following SQL to create an external stream to read live data from Redpanda.

```sql
CREATE EXTERNAL STREAM frontend_events(raw string)
SETTINGS type='kafka',
         brokers='redpanda:9092',
         topic='owlshop-frontend-events';
```

Open the `clickhouse client` in the clickhouse container. Run the following SQL to create a regular MergeTree table.

```sql
CREATE TABLE events
(
    _tp_time DateTime64(3),
    url String,
    method String,
    ip String
)
ENGINE=MergeTree()
PRIMARY KEY (_tp_time, url);
```

Go back to `proton client`, run the following SQL to create an external table to connect to ClickHouse:
```sql
CREATE EXTERNAL TABLE ch_local
SETTINGS type='clickhouse',
         address='clickhouse:9000',
         table='events';
```

Then create a materialized view to read data from Redpanda, extract the values and turn the IP to masked md5, and send data to the external table. By doing so, the transformed data will be written to ClickHouse continuously.

```sql
CREATE MATERIALIZED VIEW mv INTO ch_local AS
    SELECT now64() AS _tp_time,
           raw:requestedUrl AS url,
           raw:method AS method,
           lower(hex(md5(raw:ipAddress))) AS ip
    FROM frontend_events;
```

## Read data from ClickHouse

You can run the following SQL to query ClickHouse:

```sql
SELECT * FROM ch_local;
```

Or apply SQL functions or group by, such as

```sql
SELECT method, count() AS cnt FROM ch_local GROUP BY method
```

Please note, Proton will read all rows with selected columns from the ClickHouse and apply aggregation locally. Check [External Table](https://docs.timeplus.com/proton-clickhouse-external-table) documentation for details.