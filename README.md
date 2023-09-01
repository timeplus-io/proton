[![NightlyTest](https://github.com/timeplus-io/proton/actions/workflows/nightly_test.yml/badge.svg?branch=develop)](https://github.com/timeplus-io/proton/actions/workflows/nightly_test.yml)

## What is Proton ?

Proton is a unified streaming and histroical data processing engine which powers the Timeplus streaming analytic platform. It is built on top of trimmed single instance ClickHouse code base. Its major goals are simplicity, efficient with good performance in both streaming and historical query processing. Users can do streaming queries and historial queries or a combination of both in one SQL. Since its major use cases focus is streaming query processing, by default, a SQL query in proton is a streaming query which means it is long running, never ends and continously tracks and evaluates the delta changes and push the query results to users or target systems.

## Architecture

The following diagram depicts the high level architecture of single instance proton. All of the components / functionalites are built into a single binary.
Users can create a stream by using `CREATE STREAM ...` SQL. Every stream has 2 parts at storage layer by default: the real-time streaming data part and the historical data part which
are backed by NativeLog and ClickHouse historical data store respectively. Fundamentally, a stream in proton is a reguar database table with a write ahead log in front, which is streaming queriable.

When users `INSERT INTO ...` data to proton, the data always first lands in NativeLog which is immediately queriable. since NativeLog is in essence a replicated write ahead log (WAL) and it is append-only, it
can support high frequent, low latency and large concurrent data ingestion work loads. In background, there is a separate thread tailing the data from NativeLog and commits the data in bigger batch
to the historical data store. Since proton leverages ClickHouse for its historical store, its historical query processing is very fast as well.

In quite lots of scenarios, data is already in Kafka / Redpanda or other streaming data hub, users can create external streams to point to the streaming data hub and do streaming query processing
directly against them and then either materialize them in Proton or send the results to external systems.

Interested users can refer [How Timeplus Unifies Streaming and Historical Data Processing](https://www.timeplus.com/post/unify-streaming-and-historical-data-processing) blog for more details regarding its academic foundation and latest industry developments.

![Proton Architecture](https://github.com/timeplus-io/proton/raw/develop/design/proton-high-level-arch.svg)

## Key Streaming Functionalities

1. Streaming Transformation
2. Streaming Join (stream to stream, stream to table join)
3. Streaming Aggregation
4. Streaming Window Processing (tumble / top / session)
5. Substream
6. Data Revision Processing
7. Federated Streaming Query
8. JavaScript UDF / UDAF
9. Materialize View

## Docs

For more streaming query functionalities, SQL syntax, functions, aggregation functions etc, see our [User Documentation](https://docs.timeplus.com/)

## Starting with Timeplus Cloud

We can run Proton for you and even provide more functionalities in Timeplus console. See our online documentation : Quickstart with [Timeplus Cloud]().

## Starting Proton with Container

## Build From Source

### Clone proton

```
git clone --recurse-submodules git@github.com:timeplus-io/proton.git
```

### Build with Docker

```
$ cd proton/docker/builder
$ make build
```

After build, you can find the compiled proton binary in `build_docker` directory.

### Bare Metal Build

#### Build Tools
- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### MacOS
We don't support build proton by using Apple Clang. Please use `brew install llvm` to install
clang-16 / clang++-16.

#### Ad-hoc Build with Default C/C++ Compilers

```
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

#### Ad-hoc build with Customized C/C++ Compilers

The following is an example on MacOS after installing clang by using home brew.

```
$ cd proton
$ mkdir -p build && cd build && cmake .. -DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++
$ ninja
```

## Run Proton Binary Locally

Enter Proton binary folder and run Proton server

```
$ cd proton/build
$ ./programs/proton server --config ../programs/server/config.yaml
```

In another console, run Proton client

```
$ cd proton/build
$ ./programs/proton client
```

### Create Stream, Ingest Data and Query

In Proton client console,  
```sql
-- Create stream
CREATE STREAM devices(device string, location string, temperature float);

-- Run the stream query and it is always long running waiting for new data
SELECT device, min(temperature), max(temperature) FROM devices GROUP BY device;
```

Launch another Proton client console

```sql
-- Insert some data
INSERT INTO devices (device, location, temperature)
VALUES
('dev1', 'ca', 57.3),
('dev2', 'sh', 37.3),
('dev3', 'van', 17.3);
```

```sql
-- Insert more data
INSERT INTO devices (device, location, temperature)
VALUES
('dev1', 'ca', 38.5),
('dev2', 'sh', 18.5),
('dev3', 'van', 88.5);
```