daisy is an open-source column-oriented and streaming time series database management system built on top of ClickHouse. At it core, it leverages the super power of single instance ClickHouse but provides almost infinite horizontal scalability in data ingestion and query in distributed mode. Daisy combines the two best worlds of real-time streaming processing and data warehouse which we called `Streaming Warehouse`. Internally, it is tailored for time series data and `time` is the first class concept in Daisy.

# Achitecture

In distributed mode, Daisy depends on a high throughput and low latency distributed write ahead log for data ingestion and streaming query. Also it depends on this distributed write ahead log for metadata management and node membership management etc tasks. This architecture is inspired by lots of prior work in academic and industry, to name a few [LinkedIn Databus](https://dl.acm.org/doi/10.1145/2391229.2391247), [SLOG](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf), and lots of work from [Martin Kleppmann](https://martin.kleppmann.com/)

achitecture highlights

1. Extremely high performant data ingestion and query. Every node in the cluster can serve ingest and query request.
2. Time as first class concept and internally the engine is optimized for it
3. Built-in streaming query capability
4. At least once delivery semantic by default. Can elimnitate data duplication in practical scenarios in distributed mode
5. Automatic data shard placement
6. Mininum operation overhead if running in K8S

![Daisy Architecture](https://github.com/datatlas-io/daisy/raw/develop/design/daisy-high-level-arch.png)

# Build

## Build Tools

- clang-11/12
- cmake
- ninja

## Ad-hoc build

```
$ cd daisy
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## Full Release Package Build

```
$ cd daisy/docker/packager
$ ./packager --package-type deb --compiler clang-11 --output-dir daisy-package-directory
```

# Install, Configure and Run

## Single Instance

### When you build Daisy from scratch

From your `build` directory

```
$ ./programs/clickhouse-server --config ../programs/config.xml
```

### If you have a deb installation package

```
$ sudo apt install -y clickhouse-*.deb
$ service clickhouse-server start
```

## Distributed Cluster

1. Install Daisy on each node
2. Install Kafka (cluster) on separated nodes and start it (the cluster)
3. Configure `config.xml` in Daisy to point to the Kafka (cluster) on each node and start Daisy on each node

```
$ sudo vim /etc/clickhouse-server/config.xml

```

Uncomment out the while `<cluster_settings>` section and fix `<!- -` as `<!--` and fix `- ->` as `-->` if there are any of them.
Then configure `brokers` to point to the deployed Kafka cluster. Users can fine tune the Kafka settings if necessary.
For example, fine tune the replication factor if users like high durability of the metadata of the system
and latest streaming data. Keeping the settings as default should be generally good for most production load.

One example configuration is as:

```
<cluster_settings>
    <streaming_storage>
        <!-- Multiple clusters of streaming storage are supported -->
        <kafka>
            <default>true</default>
            <cluster_name>default-sys-kafka-cluster-name</cluster_name>
            <!-- cluster_id canâ€™t be changed after the system is up and running -->
            <!-- because the code depends on the immutability of cluster_id -->
            <cluster_id>default-sys-kafka-cluster-id</cluster_id>
            <security_protocol>PLAINTEXT</security_protocol>
            <brokers>node1_ip1:9092,node2_ip2:9092,node3_ip3:9092</brokers>
            <replication_factor>2</replication_factor>

            <!-- Global settings for both producers & consumers -->
            <!--
            <sasl_kerberos_keytab>...</sasl_kerberos_keytab>
            <sasl_kerberos_principal>...</sasl_kerberos_principal>
            -->

            <topic_metadata_refresh_interval_ms>300000</topic_metadata_refresh_interval_ms>
            <message_max_bytes>1000000</message_max_bytes>
            <statistic_internal_ms>30000</statistic_internal_ms>
            <debug></debug>

            <!-- Global settings for producers -->
            <enable_idempotence>true</enable_idempotence>
            <queue_buffering_max_messages>100000</queue_buffering_max_messages>
            <queue_buffering_max_kbytes>1048576</queue_buffering_max_kbytes>
            <queue_buffering_max_ms>5</queue_buffering_max_ms>
            <message_send_max_retries>2</message_send_max_retries>
            <retry_backoff_ms>100</retry_backoff_ms>
            <compression_codec>snappy</compression_codec>

            <!-- Global librdkafka client settings for producers -->
            <message_timeout_ms>40000</message_timeout_ms>
            <message_delivery_async_poll_ms>100</message_delivery_async_poll_ms>
            <message_delivery_sync_poll_ms>10</message_delivery_sync_poll_ms>

            <!-- Global settings for consumers -->
            <group_id>daisy</group_id>
            <message_max_bytes>1000000</message_max_bytes>
            <enable_auto_commit>true</enable_auto_commit>
            <check_crcs>false</check_crcs>
            <auto_commit_interval_ms>5000</auto_commit_interval_ms>
            <fetch_message_max_bytes>1048576</fetch_message_max_bytes>

            <!-- Global librdkafka client settings for consumers -->
            <queued_min_messages>1000000</queued_min_messages>
            <queued_max_messages_kbytes>65536</queued_max_messages_kbytes>

            <internal_pool_size>2</internal_pool_size>
        </kafka>
    </streaming_storage>

    <!-- system is for distributed DDL -->
    <system_ddls>
        <name>__system_ddls</name>
        <replication_factor>3</replication_factor>
        <!-- 168 hours (7 days) -->
        <data_retention>168</data_retention>
    </system_ddls>

    <!-- system_catalog is for distributed catalog service -->
    <!-- it is compact -->
    <system_catalogs>
        <name>__system_catalogs</name>
        <replication_factor>3</replication_factor>
    </system_catalogs>

    <!-- system_node_metrics is for distributed placement service -->
    <!-- it is compact -->
    <system_node_metrics>
        <name>__system_node_metrics</name>
        <replication_factor>1</replication_factor>
    </system_node_metrics>

    <!-- system_task_dwal is for global task status -->
    <system_tasks>
        <name>__system_tasks</name>
        <replication_factor>2</replication_factor>
        <!-- 24 hours -->
        <data_retention>24</data_retention>
    </system_tasks>

    <!-- In the cluster, assigne one and only one node with `placement and ddl` role.
         Assign one and only one node with `tasks` role.
         Every node in the cluster shall have `catalog` role.
         Note `placement, ddl and tasks` roles can coexist on the same node.
    -->
    <node_roles>
        <role>placement</role>
        <role>catalog</role>
        <role>ddl</role>
        <role>task</role>
    </node_roles>

</cluster_settings>
```

## Create Distributed Table

REST API

```
curl  http://daisy-ip:8123/dae/v1/ddl/tables -X POST -d '{
    "name": <table-name, String>,
    "shards": <number-of-shards, Integer>,
    "replication_factor": <number-of-replicas-per-shard, Integer>,
    "columns": [{"name" : <column-name, String>, "type" : <column-type, String>, "default" : <default-value-or-expression, String>}, ...],
    shard_by_expression": <sharding-expression-for-table, String>,
    index_time_bucket_granularity": <String>,
    partition_time_bucket_granularity": <String>,
    _time_column: <column-for-time, String>
}'
```

The following example is creating a `testtable` with 2 columns and all other settings are default

```
$ curl http://localhost:8123/dae/v1/ddl/tables -X POST -d '{
    "name" : "testtable",
    "columns": [{"name": "i", "type": "Int32"}, {"name": "timestamp", "type": "Datetime64(3)", "default" : "now64(3)"}]
}'
```

## Ingest Data

```
$ curl http://localhost:8123/dae/v1/ingest/tables/testtable -X POST -H "content-type: application/json" -d '{
    {"columns": ["i"], "data": [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]]}
}'
```

## Query Data

### Query via client


```
$ clickhouse-client
```

And query like `SELECT * FROM testtable`

### Query Data via REST API
```
$ curl http://localhost:8123/dae/v1/search -X POST -H "content-type: application/json" -d '{"query": "SELECT * FROM testtable"}'
```