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

## Avro

If you want to generate CDC messages in Avro format and consume the data in Proton, you need to:

1. Add additional JARs to the `debezium/connect` docker image. 

   > Beginning with Debezium 2.0.0, Confluent Schema Registry support is not included in the Debezium containers ([Source](https://debezium.io/documentation/reference/stable/configuration/avro.html#confluent-schema-registry))

2. Create the connector job with AvroConverter and schema registry.
3. Create an external stream in Proton with proper columns and settings.

The detailed steps are:

### Add additional JARs to debezium container

Beginning with Debezium 2.0.0, Confluent Schema Registry support is not included in the Debezium containers. You need to download those JARs and add to the class path.  The Debezium [documeantion](https://debezium.io/documentation/reference/stable/configuration/avro.html#confluent-schema-registry) shared the breif steps. But the list of JAR is not complete. The following ones are required:

1. avro-1.11.3.jar
2. common-config-7.6.0.jar
3. common-utils-7.6.0.jar
4. failureaccess-1.0.1.jar
5. guava-32.0.1-jre.jar
6. kafka-avro-serializer-7.6.0.jar
7. kafka-connect-avro-converter-7.6.0.jar
8. kafka-connect-avro-data-7.6.0.jar
9. kafka-schema-converter-7.6.0.jar
10. kafka-schema-registry-client-7.6.0.jar
11. kafka-schema-serializer-7.6.0.jar

To download those JARs, you can first install the maven tool, then run the following commands:

```shell
mvn dependency:get -DremoteRepositories=https://packages.confluent.io/maven -Dartifact=io.confluent:kafka-connect-avro-converter:7.6.0

mvn dependency:get -DremoteRepositories=https://packages.confluent.io/maven -Dartifact=io.confluent:common-config:7.6.0
```

Then go to `~/.m2/repository` folder and obtain those JARs.

When the docker compose is running, use the Docker Desktop, open the `connect-1` container. In the **Files** tab, go to /kafka/connect/debezium-connector-mysql folder. Right click to choose **Import** option and add those 11 JAR files to the folder. Restart the connect container.

### Create new connector job

The following command will create a new job. Using Redpanda's built-in schema registry. `io.confluent.connect.avro.AvroConverter` is available after you add extra JAR to the connect container. If you get any ClassNotFound or NoClassDef error, please make sure you have followed the previous steps and restart the connect container.

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
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url":"http://redpanda:8081",
    "value.converter.schema.registry.url":"http://redpanda:8081"
  }
}'
```

### Create an external stream in Proton

Since Proton 1.5, the schema registry with Avro format is supported. Run the following SQL to create an external stream:

```sql
CREATE EXTERNAL STREAM customers_avro(op string)
SETTINGS 	type='kafka',
          brokers='redpanda:9092',
          topic='dbserver1.inventory.customers',
          data_format='Avro',
          kafka_schema_registry_url='http://redpanda:8081';
```

To query the result, for example, list existing data, you can run the SQL

```sql
select * from customers_avro where _tp_time>earliest_ts()
```

Please note, there is an issue to get nullable data in Proton today. We are fixing this.
