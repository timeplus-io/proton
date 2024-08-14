-- read the topic via an external stream
CREATE EXTERNAL STREAM customers_cdc(raw string)
                SETTINGS type='kafka',
                         brokers='redpanda:9092',
                         topic='dbserver1.inventory.customers';

-- create an external table so that Timeplus can write to ClickHouse
CREATE EXTERNAL TABLE customers
SETTINGS type='clickhouse',
        address='clickhouse:9000',
        table='customers';

-- create a materialized view as a streaming ETL job
CREATE MATERIALIZED VIEW mv_mysql2ch INTO customers AS
    SELECT
           raw:payload.after.id::int32 AS id,
           raw:payload.after.first_name AS first_name,
           raw:payload.after.last_name AS last_name,
           raw:payload.after.email AS email
    FROM customers_cdc WHERE raw:payload.op in ('r','c') SETTINGS seek_to='earliest';
