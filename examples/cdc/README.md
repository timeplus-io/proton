# Demo for CDC(Change Data Capture) with Debezium/Redpanda/Proton

This docker compose file demonstrates how to capture live database change from a OLTP database(e.g. MySQL) and apply real-time analytics with Proton.

## Start the example

Simply run `docker compose up` in this folder. Five docker containers in the stack:
1. ghcr.io/timeplus-io/proton:latest, as the streaming database.
2. docker.redpanda.com/redpandadata/redpanda, as the Kafka compatiable streaming message bus
3. docker.redpanda.com/redpandadata/console, as the web UI to explore data in Kafka/Redpanda
4. debezium/connect, as the CDC engine to read changes from OLTP and send data to Kafka/Redpanda
5. debezium/example-mysql, a pre-configured MySQL

## Create the CDC job
Perform the following command in your host server, since port 8083 is exposed from Debezium Connect.
```shell
curl --request POST \
  --url http://localhost:8083/connectors \
  --header 'Content-Type: application/json' \
  --data '{
  "name": "inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "dbserver1",
    "database.include.list": "inventory",
    "schema.history.internal.kafka.bootstrap.servers": "redpanda:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory"
  }
}'
```

## Run SQL

You can use `docker exec -it <name> proton-client -m -n` to run the SQL client in Proton container. Or use the Docker Desktop UI to choose the container, choose "Exec" tab and type `proton-client` to start the SQL client.

Copy all content in cdc.sql and paste in the Proton Client.

Run
```sql
select * from customers
```
to see the current data.

Use a MySQL client(e.g. DBeaver) to add/update/delete some records to see the update from `select * from customers`. You can also run `select * from table(customers)` to avoid waiting for new updates.