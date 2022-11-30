`proton` is a column-oriented and streaming time series database management system built on top of ClickHouse. At it core, it leverages the super power of single instance ClickHouse but provides almost infinite horizontal scalability in data ingestion and query in distributed mode. `proton` combines the two best worlds of real-time streaming processing and data warehouse which we called `Streaming Warehouse`. Internally, it is tailored for time series data and `time` is the first class concept in `proton`.

# Architecture

In distributed mode, `proton` depends on a high throughput and low latency distributed write ahead log for data ingestion and streaming query. It also depends on this distributed write ahead log for metadata management and node membership management etc tasks.

architecture highlights

1. Extremely high performant data ingestion and query. Every node in the cluster can serve ingest and query request.
2. Time as first class concept and internally the engine is optimized for it
3. Built-in streaming query capability
4. At least once delivery semantic by default. Can eliminate data duplication in practical scenarios in distributed mode
5. Automatic data shard placement
6. Minimal operation overhead if running in K8S

![Proton Architecture](https://github.com/timeplus-io/proton/raw/develop/design/proton-high-level-arch.png)

# Build

## Clone proton

```
git clone --recurse-submodules git@github.com:timeplus-io/proton.git
```

## Build with Docker

```
$ cd proton/docker/builder
$ make build
```

After build, you can find the compiled `proton` binaries in `build_docker` directory.

## Bare Metal Build

### Build Tools
- clang-15 and above
- cmake
- ninja

### MacOS
We don't support build proton by using Apple Clang. Please use `brew install llvm` to install 
clang-15 / clang++-15.

### Ad-hoc Build with Default C/C++ Compilers

```
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

### Ad-hoc build with Customized C/C++ Compilers

The following is an example on MacOS after installing clang by using home brew.
```
$ cd proton
$ mkdir -p build && cd build && cmake .. -DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++
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

The following example is creating a `devices` with several columns and all other settings are default

```
curl http://localhost:8123/proton/v1/ddl/tables -X POST -d '{
   "name": "devices",
   "columns": [
      {
         "name": "device",
         "type": "String"
      },
      {
         "name": "location",
         "type": "String"
      },
      {
         "name": "temperature",
         "type": "Float32"
      },
      {
         "name": "timestamp",
         "type": "Datetime64(3)",
         "default": "now64(3)"
      }
   ]
}'
```

## Ingest Data

### Ingest Data via CLI 

Run `proton-client` console interactively
```shell
$ docker run --rm --network=timeplus-net -it timeplus/proton /bin/bash
$ proton-client --host proton-server -m
```

```sql
# Launch proton-client if not yet 

INSERT INTO devices (device, location, temperature, timestamp) 
VALUES 
('dev1', 'ca', 57.3, '2020-02-02 20:00:00'), 
('dev2', 'sh', 37.3, '2020-02-03 12:00:00'),
('dev3', 'van', 17.3, '2020-02-02 20:00:00');
```

### Ingest Data via REST API

```
curl http://localhost:8123/proton/v1/ingest/tables/devices -X POST -H "content-type: application/json" -d '{
    "columns": ["device", "location", "temperature", "timestamp"],
    "data": [
        ["dev1", "ca", 57.3, "2020-02-02 20:00:00"],
        ["dev2", "sh", 37.3, "2020-02-03 12:00:00"],
        ["dev3", "van", 17.3, "2020-02-02 20:00:00"]
    ]
}'
```

## Query Data

### Query via CLI 

```sql
# Launch proton-client if not yet 

SELECT * FROM devices
```

### Query Data via REST API

```
curl http://localhost:8123/proton/v1/search -H "content-type: application/json" -d '{"query": "SELECT * FROM devices"}'
```

### Streaming Query

Simple tail

```
docker run --rm --network=timeplus-net timeplus/proton \
    proton-client --host proton-server \
    --query 'SELECT * FROM devices WHERE temperature > 50.0 EMIT STREAM'
```

Interactive streaming query

```sql
# Launch proton-client if not yet
# Tumbling aggregation

timeplus :) SELECT device, avg(temperature) as avg_temp
FROM tumble(devices, INTERVAL 3 SECOND)
WHERE temperature > 50.0 GROUP BY device, wstart, wend
EMIT STREAM;

# Use a different time column for windowing

timeplus :) SELECT device, avg(temperature) as avg_temp
FROM tumble(devices, timestamp, INTERVAL 3 SECOND)
WHERE temperature > 50.0 GROUP BY device, wstart, wend
EMIT STREAM;
```

For more streaming query, please refer to [Proton Streaming Processing Spec](spec/streaming.md)
