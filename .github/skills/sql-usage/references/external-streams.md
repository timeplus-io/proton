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

## Other types

| Type | Availability | Purpose |
|------|-------------|---------|
| `log` | Experimental | Local log file streaming |
