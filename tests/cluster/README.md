# Probe

----

Probe is a testing tool that supports proton smoke tests, data generation and statistic of query (stress test will be supported in the future).

----

<!-- TOC -->
* [Probe](#probe)
  * [Prerequisites](#prerequisites)
    * [How to download private repo timeplus-io/pt-go](#how-to-download-private-repo-timeplus-iopt-go)
      * [Using HTTPS to Download](#using-https-to-download)
      * [Using SSH to Download](#using-ssh-to-download)
  * [How to use Probe](#how-to-use-probe)
    * [How to write a smoke test case](#how-to-write-a-smoke-test-case)
      * [query step](#query-step)
      * [stream step](#stream-step)
      * [check step](#check-step)
      * [wait step](#wait-step)
      * [command step](#command-step)
      * [rest step](#rest-step)
      * [pulsar step](#pulsar-step)
      * [minio step](#minior-step)
      * [Data Type Conversion Table](#data-type-conversion-table)
    * [How to generate data and insert into proton/redpanda](#how-to-generate-data-and-insert-into-protonredpanda)
      * [Generator](#generator)
      * [Proton Dataloader](#proton-dataloader)
      * [Kafka Dataloader](#kafka-dataloader)
    * [How to statistic query](#how-to-statistic-query)
<!-- TOC -->

## Prerequisites

- Go 1.21 or higher
- Have access to private repo timeplus-io/pt-go

### How to download private repo timeplus-io/pt-go

Set Go environment variable `GOPRIVATE` to `github.com/timeplus-io/pt-go` to download the private repo.

`export GOPRIVATE=github.com/timeplus-io/pt-go`

#### Using HTTPS to Download
Create a .netrc file in your home directory and set your GitHub username and password or personal access token:
```
machine github.com
login <username>
password <password or personal access token>
```

#### Using SSH to Download
Add the following configuration to your ~/.gitconfig to replace HTTPS download with SSH:
```
[url "ssh://git@github.com/"]
insteadOf = https://github.com/
```

## How to use Probe

1. run proton and kafka/redpanda.

2. write message of cluster to `deployment/{cluster_name}.yaml`.

Probe requires a cluster configuration file to run. The configuration can be defined in a few ways, as explained below. The configuration is sourced from three locations, with priority levels for each:

| Priority | Configuration Source | Description |
|----------|----------------------|-------------|
| 1        | Command-Line `-d` argument | A specific configuration provided via command line. <br> It will merge and override the folder-based configuration and local configuration. |
| 2        | Folder `deployment` inside the directory | A specified configuration based on directory. It takes effect when folder `deployment` within the specified directory specified exists. <br> It will merge and override the local configuration. |
| 3        | Local deployment configuration (`$HOME/.probe/deployment`) | A local configuration file located at `$HOME/.probe/deployment` directory. <br> It will always be applied unless overridden by other sources. |

To specify which cluster the test should run on, you need to use the `-c` command parameter. Otherwise, the test will be executed in parallel across all loaded clusters.

Example
```shell
./probe smoke -v example -d cluster/examples -c p3k1 # config from 'cluster/examples' folder
./probe smoke -v example -c p3k1 # config from 'example/deployment' folder
./probe smoke -v example -c p3k1 # config from '$HOME/.probe/deployment' when 'example/deployment' not exist 
```

Each node has 5 kinds of services: proton, kafka, pulsar, minio and ssh.

Here is an example of `deployment/p3k1.yaml`, other examples in `example/deployment`:

```yaml
p1:
   host: localhost
   ssh:
      port: 22
      user: root
      password:
      public_key: id_rsa
   proton:
      cluster_id:
      role:
         - metadata
         - data
      username: default
      password:
      database: default
      tcp_port: 8463
      http_port: 3218
      tcp_snap_port: 7587
      http_snap_port: 8123
      postgres_port: 5432
      prometheus_port: 9363
p2:
   host: localhost
   ssh:
      port: 22
      user: root
      password:
      public_key: id_rsa
   proton:
      cluster_id:
      role:
         - metadata
         - data
      username: default
      password:
      database: default
      tcp_port: 8473
      http_port: 3228
      tcp_snap_port: 7597
      http_snap_port: 8133
      postgres_port: 5442
      prometheus_port: 9363
p3:
   host: localhost
   ssh:
      port: 22
      user: root
      password:
      public_key: id_rsa
   proton:
      cluster_id:
      role:
         - metadata
         - data
      username: default
      password:
      database: default
      tcp_port: 8483
      http_port: 3238
      tcp_snap_port: 7507
      http_snap_port: 8143
      postgres_port: 5452
      prometheus_port: 9363
k1:
   host: localhost
   ssh:
      port: 22
      user: root
      password:
      public_key: id_rsa
   kafka:
      brokers:
         - "k1:9092"
      tls: false
      sasl: plaintext
      username: user
      password: password
   pulsar:
      service_url: pulsar://k1:6650
      web_service_url: http://k1:8080
m1:
  host: localhost
  minio:
    endpoint: localhost:9000
    access_key_id: minioroot
    secret_key: minioroot
```

3. write a test suite, examples are in `example`.

Now we have  two types of test suite formats:

- Single YAML File

In this format, the test suite is defined within a single YAML file. This file contains any number of test cases, and each test case includes any number of test steps.

`query.yaml`:

```yaml
tags:
   - demo
description: smoke test
setup:
   scope: suite
   steps:
      - type: query
        sql: create stream if not exists test20_limit_by_stream(i int32, s string)

      - type: wait
        time: 2
teardown:
   scope: suite
   steps:
      - type: query
        sql: drop stream if exists test20_limit_by_stream

      - type: wait
        time: 2

cases:
   - id: 0
     tags:
        - demo
     name: case1
     description: description of case1
     cluster:
        - p1 # Correspond with p1.yaml in deployment folder
     steps:
        - type: query
          node:
             - p1
          sql: insert into test20_limit_by_stream(i, s) values (1, 's1'), (2, 's2'), (3, 's3'), (4, 's4')

        - type: stream
          name: "5"
          node:
             - p1
          query_id: '1436'
          query: select i, s from test20_limit_by_stream
          schema:
             - name: i
               type: int32
             - name: s
               type: string

        - type: wait
          time: 4

        - type: query
          node:
             - p1
          sql: insert into test20_limit_by_stream(i, s) values
          schema:
             - name: i
               type: int32
             - name: s
               type: string
          inputs:
             - [21, 'ca1']
             - [22, 'ca2']
             - [23, 'ca3']

        - type: wait
          time: 3

        - type: check
          target_name: "5"
          expected_result:
             - [21, 'ca1']
             - [22, 'ca2']
             - [23, 'ca3']

   - id: 1
     tags:
        - demo
     name: case2
     description: description of case2
     cluster:
        - p1
     steps:
        - type: query
          node:
             - p1
          sql: drop stream if exists test20_limit_by_stream    
```

- Directory Structure

In this format, the test suite information is stored in a directory. The suite information, setup, and teardown steps are defined in a config.yaml file within the directory. 

Example structure:

```bash
/suite_directory
- config.yaml   # Contains suite information, setup, and teardown
- case1.yaml    # Contains TestCase1
- case2.yaml    # Contains TestCase2
```

4. build probe and run:

```bash
cd probe
go build .
# ./probe smoke [flag] dir
./probe smoke -v -c p1 -s query ../example
```

Flags:

| short | long            | arg type | description                                            | default     |
|-------|-----------------|----------|--------------------------------------------------------|-------------|
| -h    | --help          |          | help for probe                                         |             |
| -b    | --blacklist     | strings  | Black list Tag of smoke(default bug,todo,skip)         |             |
| -c    | --cluster       | strings  | Cluster name                                           |             |
|       | --collect-only  | bool     | Whether only collect cases                             | false       |
| -d    | --deployment    | string   | Deployment folder(default in dir/deployment)           |             |
| -k    | --case          | strings  | Case name                                              |             |
| -r    | --retry         | int      | Max retry times of failed case                         |             |
| -s    | --suite         | strings  | Suite name                                             |             |
| -t    | --tag           | strings  | Tag of smoke test                                      |             |
| -o    | --os            | strings  | Os of smoke test                                       |             |
| -a    | --arch          | strings  | Arch of smoke test                                     |             |
| -f    | --log-file      | string   | Log File path                                          |             |
|       | --log-level     | string   | log level, e.g. debug, info, warn, error, fatal, panic | info        |
| -v    | --verbose       |          | verbose mode, set log level=debug                      |             |
|       | --variable      | maps     | Variable configured in templated case steps            |             |

### How to write a smoke test case

Smoke test case in probe consists of a series of steps.

A basic step is defined as follows:

```yaml
- type: <step_type>
  name: <step_name> # it is optional, it should be set if other steps need this step's result as input
  node: # it is optional, it should be set to control the node to execute the step(if num of nodes > 1 it will select one randomly)
     - <node_name> # Correspond with node name in deployment file
     - <node2>
```

#### query step

Query step is used to execute a sql(insert,create...) and only outputs error message.

```yaml
- type: query
  sql: create stream if not exists test20_limit_by_stream(i int32, s string)

- type: query
  sql: insert into test20_limit_by_stream(i, s) values (1, 's1'), (2, 's2'), (3, 's3'), (4, 's4')

# another format of insert query, schema and inputs are must
- type: query
  sql: insert into test20_limit_by_stream(i, s) values
  schema:
   - name: i
     type: int32
   - name: s
     type: string
  inputs:
    - [21, 'ca1']
    - [22, 'ca2']
    - [23, 'ca3']
```

#### stream step

Stream step is used to execute query(stream and table). It's async, so it can't be the last step.

```yaml
- type: stream
  name: "5"
  query_id: '1436' # optional, if not set, it will be generated randomly
  query: select i, s from test20_limit_by_stream
  schema:
     - name: i
       type: int32
     - name: s
       type: string
```

#### check step

Check step is used to check the result of other steps.

```yaml

- type: check
  target_name: "5"
  mode: random # optional, default is sequence, it will check the result in random order
  timeout: 15 # optional, in seconds, default is 10
  expected_result:
     - [21, 'ca1']
     - [22, 'ca2']
     - [23, 'ca3']
```

#### wait step

Wait step is used to wait the create query or control async step.

```yaml
- type: wait
  time: 4
```

#### command step

Command step uses ssh connect to the target node and execute shell command.

```yaml
- name: param-command-1
  type: command
  node:
    - p1
  shell: ls -ltrh /var/lib/proton | grep nativelog | awk '{print $9}'
```

#### rest step

Rest step is used to send a rest request.

```yaml
  - type: rest
    node:
      - p1
    method: POST
    url: /proton/v1/ddl/streams
    data:
      name: udf_types
      type: table
      columns:
        - name: i
          type: int
        - name: i8
          type: int8
        - name: f32
          type: float32
        - name: s
          type: string
        - name: dt
          type: datetime64(6)
          default: now64(6)
```

#### pulsar step

Pulsar step is used to manage topics in Pulsar and produce messages to Pulsar topics.

```yaml
  # create a topic
  - type: pulsar
    name: create
    topic: my-topic # or use full name like persistent://<tenant>/<namespace>/my-topic
    partitions: 3 # optional, if not specified, it will create a non-partitioned topic

  # delete a non-partitioned topic
  - type: pulsar
    name: delete
    topic: my-topic # or use full name like persistent://<tenant>/<namespace>/my-topic

  # delete a partitioned topic
  - type: pulsar
    name: delete-partitioned
    topic: my-topic # or use full name like persistent://<tenant>/<namespace>/my-topic

  # produce messages to a topic/partition
  - type: pulsar
    name: produce
    # to produce to a partition, use the partition topic directly, like:
    # topic: my-topic-partition-0
    topic: my-topic # or use full name like persistent://<tenant>/<namespace>/my-topic
    messages:
      - id: something
        value: 4.13
```

#### minio step

Minio step is used to manage buckets in Minio.

```yaml
  # delete a bucket
  - type: minio
    name: delete
    bucket: test-bucket

  # create a bucket
  - type: minio
    name: create
    bucket: test-bucket
```

#### Data Type Conversion Table

- Proton Type: the type in proton
- Probe Type: the type written in the type field of the schema
- Go Type: the type in `column` folder

demo in `example/datatype`:
`probe smoke -v -c p1 -s datatype example`

|            Proton Type             |             Probe type             |          Go type           |                                   Note                                   |
|:----------------------------------:|:----------------------------------:|:--------------------------:|:------------------------------------------------------------------------:|
|          (u)int8/16/32/64          |          (u)int8/16/32/64          |      (u)int8/16/32/64      |                      int in schema should be int32                       |
|           (u)int128/256            |           (u)int128/256            |        math/big.Int        |                                                                          |
|          float32/float64           |          float32/float64           |      float32/float64       |                    float in schema should be float32                     |
|     decimal(precision, scale)      |     decimal(precision, scale)      | shopspring/decimal.Decimal |                                                                          |
|  decimal32/64/128/256(precision)   |     decimal(precision, scale)      | shopspring/decimal.Decimal |      relation between size and scale 32->9,64-> 18, 128->38,256->76      |
|                bool                |            bool/boolean            |            bool            |                                                                          |
|               string               |               string               |           string           |                                                                          |
|          fixed_string(N)           |          fixed_string(N)           |           []byte           |                                                                          |
|         low_cardinality(T)         |         low_cardinality(T)         |   column.LowCardinality    |                                                                          |
|                uuid                |                uuid                |      google/uuid.uuid      |                                                                          |
|                ipv4                |                ipv4                |           uint8            |                                                                          |
|                ipv6                |                ipv6                |          byte[16]          |                         same as fixed_string(16)                         |
|                date                |                date                |           uint16           |                               '2022-03-24'                               |
|              datetime              |              datetime              |           uint32           |                          '2022-03-24 17:17:17'                           |
| datetime64(precision, [time_zone]) | datetime64(precision, [time_zone]) |           int64            |                          '2020-02-02 20:01:00'                           |
|              array(T)              |              array(T)              |        []interface         |                                                                          |
|              map(k,v)              |              map(k,v)              |            map             |                               {'key1':111}                               |
|          tuple(a T1,b T2)          |          tuple(a T1,b T2)          |           []/map           | all columns have name it will be stored as map in go,else it is an array |
|            nullable(T)             |              nullable              |            nil             |                                                                          |
|         nested(a T1,b T2)          |        array(T1)/array(T2)         |             []             |                         select a.a,a.b from ...                          |
|   enum8/16('a'=1, 'b'=2, 'z'=26)   |   enum8/16('a'=1, 'b'=2, 'z'=26)   |      column.Enum8/16       |                                                                          |

### How to generate data and insert into proton/redpanda

#### Generator

Probe use generator step to generate data.

```yaml

- type: query
  sql: create stream test_gen (id int64, name string, age int64, float float64, s string, t string, u int64, v int64, w int64, x datetime64(3)) settings shards=1,replication_factor=1,sharding_expr='rand()'

- type: wait
  time: 3

- type: generator
  name: gen # it is must
  batch_size: 1500 # control the batch size of the generator(each time generate a batch of data)
  buffer_size: 2000 # control the buffer size of the generator(one goroutine uses one buffer)
  concurrency: 10 # control the number of goroutines to generate data
  interval_ms: 0 # control the interval between generating two batches
  primary_key: rd_int # it is optional
  schema:
    - name: id
      type: int64
      rule: # generator rule
        type: random_int
        params:
          min: 1
          max: 100
    - name: name
      type: string
      rule:
        type: random_string
        params:
          min_len: 5
          max_len: 10
```

Probe supports the following generator rules, develop specified generator rule can refer to `utils/rule`:

```yaml
- type: random_int
  params:
    min: 1
    max: 100
    
- type: random_string
  params:
      min_len: 5
      max_len: 10
      prefix: "prefix"

- type: random_float
  params:
      min: 1.0
      max: 100.0

- type: current_time
  
- type: random_choice
  params:
      range:
        - "a"
        - "b"
        - "c"

# random_fsm is a finite state machine, it will generate a sequence of data according to the graph
- type: random_fsm
  params:
      start_node: a
      graph: |
        a -> b 0.5
        b -> c 0.5
        b -> d 0.3
        d -> a 0.5
        c -> e 0.7
        e -> d 0.4

# random_walk means the data will change based on the previous data
- type: random_walk_int
  params:
      delta_min: -3
      delta_max: 3
      start: 10

- type: random_walk_float
  params:
      delta_min: -3.0
      delta_max: 3.0
      start: 10.0

- type: random_walk_time
  params:
      delta_min: 2s
      delta_max: 10s
      start: 2024-04-22T19:24:23Z
```

#### Proton Dataloader

Probe use proton dataloader step to directly insert data into proton.

```yaml
- type: proton_dataloader
  source: gen # the name of generator
  sink: test_gen #target stream name
  concurrency: 10
```

#### Kafka Dataloader

Data can be formatted in JSONEachROW or Protobuf or Avro and sent to Kafka.

```yaml
# 1. JSONEachRow format
- type: json_format
  name: format
  source: gen
  concurrency: 8
  buffer_size: 2000

- type: kafka_dataloader
  source: format # the name of formatter
  sink: test #target topic name in kafka(should be created before)
  concurrency: 8

# 2. Protobuf format
- type: query
  sql: |
    CREATE OR REPLACE FORMAT SCHEMA schema_name AS '
    syntax = "proto3";
    message Sch {
      int64  k = 1;
      string v = 2;
    }
    ' TYPE Protobuf
    
- type: wait
  time: 1

# you should change the brokers to your own kafka brokers!!!
- type: query
  sql: |
    CREATE EXTERNAL STREAM test_proto(
      k int64,
      v string
    )
    SETTINGS type='kafka',
    brokers='broker1:9092',
    topic='test',
    data_format='Protobuf',
    format_schema='schema_name:Sch'

- type: wait
  time: 2

- type: stream
  name: '100'
  query: select * from test_proto
  schema:
    - name: k
      type: int64
    - name: v
      type: string

- type: wait
  time: 1

# Directly input data
- type: protobuf_format
  name: format
  proto: |
    syntax = "proto3";
    message Sch {
      int64  k = 1;
      string v = 2;
    }
  class: Sch
  schema:
    - name: k
      type: int64
    - name: v
      type: string
  inputs:
    - [1, "a"]
    - [2, "b"]
    - [3, "c"]
  buffer_size: 2 # >=2

# Get data from generator
- type: protobuf_format
  name: format
  proto: |
    syntax = "proto3";
    message Sch {
      int64  k = 1;
      string v = 2;
    }
  class: Sch
  source: gen
  buffer_size: 2 # >=2
  concurrency: 1

- type: kafka_dataloader
  source: format
  sink: test
  concurrency: 1

# 3. Avro format
# create external stream with format_schema
- type: query
  sql: |
    CREATE EXTERNAL STREAM test_avro(
      k int32,
      v string
    )
    SETTINGS type = 'kafka',
    brokers = 'broker1:9092',
    topic = 'avro',
    data_format = 'Avro',
    format_schema = 'avro_schema';

# create external stream with kafka_schema_registry_url
- type: query
  sql: |
    CREATE EXTERNAL STREAM test_avro_schema(
      k int32,
      v string
    )
    SETTINGS type = 'kafka',
    brokers = 'broker1:9092',
    topic = 'avro-schema',
    data_format = 'Avro',
    kafka_schema_registry_url = 'http://k1:8081';

# Directly input data
- type: avro_format
  name: format
  avro: |
    {
      "type": "record",
      "name": "Person",
      "fields": [
          {"name": "k", "type": "int"},
          {"name": "v", "type": "string"}
      ]
    }
  schema_id: 1 # depends on external stream: (1) format_schema: omit schema_id (2) kafka_schema_registry_url: set schema_id to 1,2,3...
  schema:
    - name: k
      type: int32
    - name: v
      type: string
  inputs:
    - [1, "aaa"]
    - [2, "bbb"]
    - [3, "ccc"]

# Get data from generator
- type: avro_format
  name: format
  avro: |
    {
      "type": "record",
      "name": "Person",
      "fields": [
          {"name": "k", "type": "long"},
          {"name": "v", "type": "string"}
      ]
    }
  schema_id: 1 # depends on external stream: (1) format_schema: omit schema_id (2) kafka_schema_registry_url: set schema_id to 1,2,3...
  source: gen
  buffer_size: 2 # >=2
  concurrency: 1
```

### How to statistic query

Probe still uses the stream step to statistic query.

set the log level to info to see the statistic result. 

run `./probe smoke --log-level=info -c p1 -s concurrent ../example`

```yaml
- type: stream
  name: query1
  query: select count() from table(test_gen) format Null # set Null format to avoid output
  retry: 1000 # run query 1000 times to calculate the average,p90,p99 latency
  concurrency: 5 # run 5 queries concurrently
  statistic_only: true # only statistic the query, no output 
  schema:
    - name: count()
      type: uint64
```
