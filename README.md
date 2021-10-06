`proton` is a column-oriented and streaming time series database management system built on top of ClickHouse. At it core, it leverages the super power of single instance ClickHouse but provides almost infinite horizontal scalability in data ingestion and query in distributed mode. `proton` combines the two best worlds of real-time streaming processing and data warehouse which we called `Streaming Warehouse`. Internally, it is tailored for time series data and `time` is the first class concept in `proton`.

# Architecture

In distributed mode, `proton` depends on a high throughput and low latency distributed write ahead log for data ingestion and streaming query. It also depends on this distributed write ahead log for metadata management and node membership management etc tasks. This architecture is inspired by lots of prior work in academic and industry, to name a few [LinkedIn Databus](https://dl.acm.org/doi/10.1145/2391229.2391247), [SLOG](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf), and lots of work from [Martin Kleppmann](https://martin.kleppmann.com/)

architecture highlights

1. Extremely high performant data ingestion and query. Every node in the cluster can serve ingest and query request.
2. Time as first class concept and internally the engine is optimized for it
3. Built-in streaming query capability
4. At least once delivery semantic by default. Can eliminate data duplication in practical scenarios in distributed mode
5. Automatic data shard placement
6. Mininum operation overhead if running in K8S

![Proton Architecture](https://github.com/timeplus-io/proton/raw/develop/design/proton-high-level-arch.png)

# Build

## Build with Docker

```
$ cd proton/docker/server
$ make build
```

After build, you can find the compiled `proton` binaries in `build_docker` directory.

## Bare Metal Build

### Build Tools
- clang-12/13
- cmake
- ninja

### Ad-hoc build

```
$ cd proton 
$ mkdir -p build && cd build && cmake ..
$ ninja
```

# Run

## Run via Docker compose
```
$ cd proton/docker/compose
$ docker-compose up
```

## Create Distributed Table

Note for a complete REST APIs, please refer to `proton/spec/rest-api`.

```
curl  http://localhost:8123/proton/v1/ddl/tables -X POST -d '{
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
$ curl http://localhost:8123/proton/v1/ddl/tables -X POST -d '{
   "name":"testtable",
   "columns":[
      {
         "name":"i",
         "type":"Int32"
      },
      {
         "name":"timestamp",
         "type":"Datetime64(3)",
         "default":"now64(3)"
      }
   ]
}'
```

## Ingest Data

```
$ curl http://localhost:8123/proton/v1/ingest/tables/testtable -X POST -H "content-type: application/json" -d '{
    "columns": ["i"], 
    "data": [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]]
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
$ curl http://localhost:8123/proton/v1/search -X POST -H "content-type: application/json" -d '{"query": "SELECT * FROM testtable"}'
```

### Query Data via CLI