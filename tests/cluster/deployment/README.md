# Probe

----

Probe is a testing tool that supports proton smoke tests, data generation and statistic of query (stress test will be supported in the future).

----

<!-- TOC -->
* [Probe](#probe)
  * [How to use Probe run smoke test](#how-to-use-probe-run-smoke-test)
  * [How to write a smoke test suite](#how-to-write-a-smoke-test-suite)
    * [How to write a smoke test case](#how-to-write-a-smoke-test-case)
      * [query step](#query-step)
      * [stream step](#stream-step)
      * [check step](#check-step)
      * [wait step](#wait-step)
      * [command step](#command-step)
      * [rest step](#rest-step)
      * [How to insert protobuf into kafka](#how-to-insert-protobuf-into-kafka)
      * [Data Type Conversion Table](#data-type-conversion-table)
      * [Example](#example)
<!-- TOC -->

## How to use Probe run smoke test

1. run proton and kafka/redpanda.

2. write config of cluster to `smoke/deployment/{cluster_name}.yaml`.

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

Each node has 3 kinds of services: proton, kafka, and ssh.

The config of cluster p3k1 in `deployment/smoke/p3k1.yaml` is for ci!

If you want to run smoke test locally, you can config the `$HOME/.probe/deployment/p3k1.yaml` in your local environment as follows:

```yaml
p1:
   host: localhost
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
   kafka:
      brokers:
         - "k1:9092"
      tls: false
      sasl: plaintext
      username: user
      password: password
```

3. deploy probe:

   1. use probe binary, you can download target version from https://github.com/timeplus-io/probe/releases
   2. use docker-compose-probe.yaml to deploy the probe, run `docker compose -f docker-compose-probe.yaml up -d`

4. run smoke test

- If you use the probe binary, you can move it to `proton-enterprise/tests/cluster`
```bash
sudo chmod +x probe

# ./probe smoke [flag] dir
./probe smoke -v -c p3k1 -s limit_by smoke
```

- If you use the probe image
```bash
# probe smoke [flag] dir
docker exec -it probe probe smoke -v -c p3k1 -s limit_by /smoke
```

Flags:

| short | long            | arg type | description                                            | default     |
|-------|-----------------|----------|--------------------------------------------------------|-------------|
| -h    | --help          |          | help for probe                                         |             |
| -b    | --blacklist     | strings  | Black list Tag of smoke(default bug,todo,skip)         |             |
| -c    | --cluster       | strings  | Cluster name                                           |             |
|       | --collect-only  | bool     | Whether only collect cases                             | false       |
| -d    | --deployment    | string   | Deployment folder(deafault in dir/deployment)          |             |
| -k    | --case          | strings  | Case name                                              |             |
| -s    | --suite         | strings  | Suite name                                             |             |
| -t    | --tag           | strings  | Tag of smoke test                                      |             |
|       | --log-level     | string   | log level, e.g. debug, info, warn, error, fatal, panic | info        |
| -v    | --verbose       |          | verbose mode, set log level=debug                      |             |
|       | --variable      | maps     | Variable configured in templated case steps            |             |

## How to write a smoke test suite

We recommend using the directory structure to write a test suite, information is stored in a directory. 

The suite information, setup, and teardown steps are defined in a config.yaml file within the directory, it's a must.

Example structure:

```bash
/suite_directory
- config.yaml   # Contains suite information, setup, and teardown
- case1.yaml    # Contains TestCase1
- case2.yaml    # Contains TestCase2
```

Example config.yaml:

```yaml
name: limit_by
description: Tests covering limit by
tags:
- smoke
setup:
  scope: suite #
  steps:
  - type: query
    sql: drop stream if exists test20_limit_by_stream

  - type: wait
    time: 2
  - type: query
    sql: create stream if not exists test20_limit_by_stream(i int, s string)
  
  - type: wait
    time: 1
```

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

#### How to insert protobuf into kafka

Data can be formatted in Protobuf and sent to Kafka.

```yaml

# Protobuf format
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
    v string)
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

- type: kafka_dataloader
  source: format
  sink: test
  concurrency: 1
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

#### Example

```yaml
tags:
- limit_by
description: global aggr limit by
cluster:
- p1k1
- p3k1
steps:
- name: '2000'
  type: stream
  query: select s, latest(i) as i, emit_version() as version from test20_limit_by_stream group by s order by i desc limit 2 by version
  schema:
  - name: s
    type: string
  - name: i
    type: int32
  - name: version
    type: int64

- type: wait
  time: 1

- type: query
  sql: insert into test20_limit_by_stream(i, s) values
  schema:
  - name: i
    type: int32
  - name: s
    type: string
  inputs:
  - [1, 's1']
  - [2, 's2']
  - [3, 's3']
  - [4, 's4']

- type: wait
  time: 2

- type: check
  target_name: '2000'
  expected_result:
  - ['s4', 4, 0]
  - ['s3', 3, 0]

```