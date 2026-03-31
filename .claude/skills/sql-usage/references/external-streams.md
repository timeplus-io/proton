# External Streams

## Kafka external stream

### Creation syntax

```sql
CREATE EXTERNAL STREAM [IF NOT EXISTS] <stream_name> (
    <col_name> <col_type>, ...
) SETTINGS
    type='kafka',
    brokers='host1:9092,host2:9092',
    topic='topic_name',
    data_format='JSONEachRow';
```

### Required settings

| Setting | Purpose |
|---------|---------|
| `type` | `'kafka'` |
| `brokers` | Comma-separated broker addresses |
| `topic` | Kafka topic name |

### Data format settings

| Setting | Values | Purpose |
|---------|--------|---------|
| `data_format` | `RawBLOB` (default), `JSONEachRow`, `CSV`, `TSV`, `ProtobufSingle`, `Protobuf`, `Avro` | Message format |
| `one_message_per_row` | `true`/`false` | Ensure each Kafka message = one JSON document |
| `format_schema` | Schema string | Required for Protobuf/Avro |
| `kafka_schema_registry_url` | URL | Schema Registry endpoint |
| `kafka_schema_registry_credentials` | `user:pass` | Registry authentication |

### Authentication settings

| Setting | Values | Purpose |
|---------|--------|---------|
| `security_protocol` | `PLAINTEXT` (default), `SASL_SSL` | Wire encryption |
| `sasl_mechanism` | `PLAIN` (default), `SCRAM-SHA-256`, `SCRAM-SHA-512`, `AWS_MSK_IAM` | Auth mechanism |
| `username` / `password` | — | Credentials for SCRAM |
| `ssl_ca_cert_file` / `ssl_ca_pem` | — | SSL certificate |
| `skip_ssl_cert_check` | `true`/`false` | Bypass SSL verification |
| `config_file` | path | External librdkafka config |
| `properties` | string | Pass-through librdkafka config (e.g., `message.max.bytes`) |

### Virtual columns (metadata)

| Column | Type | Purpose |
|--------|------|---------|
| `_tp_time` | datetime64 | Message timestamp |
| `_tp_message_key` | string | Message key |
| `_tp_message_headers` | map | Key-value headers |
| `_tp_sn` | int64 | Message offset |
| `_tp_shard` | int32 | Partition ID |

### Query-level settings

| Setting | Example | Purpose |
|---------|---------|---------|
| `shards` | `'0'`, `'0,2'` | Read from specific partitions |
| `seek_to` | `'earliest'` | Read all historical messages |
| `seek_to` | `'5,3,11'` | Seek to specific offsets per partition |
| `seek_to` | `'2025-01-01T00:00:00.000'` | Timestamp-based rewind |

### Examples

```sql
-- Raw message ingestion
CREATE EXTERNAL STREAM ext_logs(raw string)
SETTINGS type='kafka', brokers='localhost:9092', topic='app_logs';

-- JSON auto-parsing
CREATE EXTERNAL STREAM ext_events(
    actor string,
    created_at datetime64(3, 'UTC'),
    id string
) SETTINGS type='kafka', brokers='localhost:9092',
    topic='github_events', data_format='JSONEachRow';

-- Extract JSON fields from raw
SELECT raw:actor AS actor, raw:created_at::datetime64(3, 'UTC') AS created_at
FROM ext_raw;

-- Access metadata
SELECT _tp_time, _tp_message_key, _tp_message_headers['trace_id']
FROM ext_events;

-- Read from specific partitions
SELECT raw FROM ext_logs SETTINGS shards='0,2';

-- Read from beginning
SELECT raw FROM ext_logs SETTINGS seek_to='earliest';
```

---

## Pulsar external stream

```sql
CREATE EXTERNAL STREAM ext_pulsar(raw string)
SETTINGS type='pulsar', service_url='pulsar://host:6650', topic='events';
```

---

## HTTP external stream (sink only)

Write data to any HTTP endpoint. Supports OpenSearch, Elasticsearch, Splunk, Datadog, Algolia, BigQuery, and any HTTP-based service.

### Creation syntax

```sql
CREATE EXTERNAL STREAM [IF NOT EXISTS] <stream_name> (
    <col_name> <col_type>, ...
) SETTINGS
    type = 'http',
    url = 'https://endpoint/path',
    data_format = '...',        -- JSONEachRow, OpenSearch, Template, CSV, etc.
    write_method = 'POST',      -- optional, default POST
    username = '...',           -- optional, HTTP basic auth
    password = '...',           -- optional
    http_header_<key> = '...';  -- optional, custom headers
```

### Write to OpenSearch / Elasticsearch

Use `data_format = 'OpenSearch'` (or alias `'ElasticSearch'`) for native bulk API integration:

```sql
CREATE EXTERNAL STREAM opensearch_sink (
    name string,
    gpa float32,
    grad_year int16
) SETTINGS
    type = 'http',
    data_format = 'OpenSearch',
    url = 'https://opensearch.company.com:9200/students/_bulk',
    username = 'admin',
    password = '...';

-- One-time insert
INSERT INTO opensearch_sink(name, gpa, grad_year)
VALUES ('Jonathan Powers', 3.85, 2025);

-- Continuous sink via materialized view
CREATE MATERIALIZED VIEW mv_to_opensearch INTO opensearch_sink AS
SELECT name, gpa, to_int16(year(now())) AS grad_year
FROM student_stream;
```

### Write to Splunk (HEC)

```sql
CREATE EXTERNAL STREAM splunk_sink (raw string)
SETTINGS
    type = 'http',
    data_format = 'RawBLOB',
    url = 'https://splunk.company.com:8088/services/collector/raw',
    http_header_Authorization = 'Splunk <hec_token>';
```

### Write to OpenObserve

```sql
CREATE EXTERNAL STREAM openobserve_sink (
    level string,
    job string,
    log string
) SETTINGS
    type = 'http',
    data_format = 'JSONEachRow',
    output_format_json_array_of_rows = 1,
    username = '...',
    password = '...',
    url = 'https://api.openobserve.ai/api/<org>/default/_json';
```

### Write with Template format (custom payload)

For services like Algolia or BigQuery that need custom JSON structure:

```sql
CREATE EXTERNAL STREAM algolia_sink (
    objectID string default uuid(),
    firstname string,
    lastname string
) SETTINGS
    type = 'http',
    url = 'https://data.us.algolia.com/2/tasks/.../push',
    data_format = 'Template',
    format_template_resultset_format = '{"action":"addObject","records":[${data}]}',
    format_template_row_format = '{"objectID":${objectID:JSON},"firstname":${firstname:JSON},"lastname":${lastname:JSON}}',
    format_template_rows_between_delimiter = ',',
    http_header_x_algolia_application_id = '...',
    http_header_x_algolia_api_key = '...';
```

### HTTP external stream settings

| Setting | Purpose |
|---------|---------|
| `type` | Must be `'http'` |
| `url` | HTTP endpoint URL |
| `data_format` | `JSONEachRow`, `OpenSearch`, `ElasticSearch`, `Template`, `CSV`, `TSV`, `RawBLOB`, `ProtobufSingle`, `Avro` |
| `write_method` | HTTP method (default `POST`) |
| `one_message_per_row` | Each POST = one row |
| `output_format_json_array_of_rows` | POST body as JSON array |
| `username` / `password` | HTTP basic auth |
| `http_header_<key>` | Custom HTTP headers |
| `skip_ssl_cert_check` | Bypass SSL verification |
| `send_timeout` / `receive_timeout` | Timeout in seconds |

---

## Iceberg external stream

Read from and write to Apache Iceberg tables via REST Catalog (AWS Glue, Gravitino, S3 Tables).

```sql
CREATE EXTERNAL STREAM iceberg_sink (
    id int64,
    name string,
    ts datetime64(3)
) SETTINGS
    type = 'iceberg',
    rest_catalog_url = 'https://glue.us-east-1.amazonaws.com/iceberg',
    warehouse = 's3://my-bucket/warehouse',
    database = 'my_db',
    table = 'my_table';
```

---

## S3 external table (sink)

Write data directly to S3 (or S3-compatible storage) with partitioning and compression:

```sql
CREATE EXTERNAL TABLE s3_sink (
    event_time datetime64(3),
    user_id string,
    action string
) SETTINGS
    type = 's3',
    url = 's3://my-bucket/events/',
    data_format = 'JSONEachRow',
    compression_method = 'gzip';
```

---

## Timeplus external stream

Cross-cluster data migration or hybrid deployment (public/private cloud, edge):

```sql
CREATE EXTERNAL STREAM remote_timeplus (
    <col_name> <col_type>, ...
) SETTINGS
    type = 'timeplus',
    hosts = 'remote-host:8463',
    stream = 'remote_stream_name';
```

---

## Sink summary

| Sink Type | Setting `type=` | Data Formats | Use Case |
|-----------|----------------|--------------|----------|
| Kafka | `kafka` | JSONEachRow, CSV, Avro, Protobuf, RawBLOB | Message queue output |
| Pulsar | `pulsar` | JSONEachRow, CSV, Avro, Protobuf, RawBLOB | Message queue output |
| HTTP (OpenSearch) | `http` | OpenSearch, ElasticSearch | Search & visualization |
| HTTP (generic) | `http` | JSONEachRow, Template, RawBLOB, CSV | Any HTTP service |
| Iceberg | `iceberg` | Native | Data lake |
| S3 | `s3` | JSONEachRow, CSV, Avro, Parquet | Object storage |
| Timeplus | `timeplus` | Native | Cross-cluster replication |

All sinks support continuous writes via materialized views:
```sql
CREATE MATERIALIZED VIEW mv_sink INTO <external_stream> AS
SELECT ... FROM <source_stream>;
```

---

## Other types

| Type | Availability | Purpose |
|------|-------------|---------|
| `log` | Experimental | Local log file streaming |
