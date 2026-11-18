# How Timeplus Proton compares to other streaming SQL systems

This page answers a question we get often ([#999](https://github.com/timeplus-io/proton/issues/999)): how does Proton compare with RisingWave, Arroyo, Apache Flink, ksqlDB, Materialize and Feldera, and when should you pick which?

It is written by the Proton maintainers, so read it with that in mind. We have tried to keep it factual: every statement about another project links to that project's own material, and the page says plainly where another system is the better choice. Facts were checked in November 2026 against Proton 3.0. If something is out of date or unfair, please open an issue or a PR.

## The short version

| If you need...                                                                 | Look at first                 |
| ------------------------------------------------------------------------------ | ----------------------------- |
| One small binary that does streaming SQL **and** fast historical analytics       | **Proton**                    |
| Streaming at the edge, on a laptop, in CI, or on a 1 vCPU box                    | **Proton**                    |
| Streaming SQL in front of Kafka/Redpanda with results you can query directly     | **Proton**, RisingWave        |
| An open-source streaming database that scales out with state on object storage  | RisingWave                    |
| PostgreSQL wire and dialect compatibility as a hard requirement                  | RisingWave, Materialize       |
| Strict consistency across views fed by OLTP change data capture                  | Materialize, Feldera          |
| Custom operators in Java/Python, the widest connector ecosystem, very large jobs | Apache Flink                  |
| Stateless or lightly stateful SQL pipelines into object storage on Cloudflare    | Arroyo (Cloudflare Pipelines) |

## At a glance

|                           | **Timeplus Proton**                                 | **RisingWave**                              | **Apache Flink**                          | **Materialize**                                  | **Arroyo**                           | **ksqlDB**                          | **Feldera**                          |
| ------------------------- | --------------------------------------------------- | ------------------------------------------- | ----------------------------------------- | ------------------------------------------------ | ------------------------------------ | ----------------------------------- | ------------------------------------ |
| **Category**              | Streaming SQL engine + columnar analytics store     | Streaming database                          | Stream processing framework               | Streaming database (incremental views)           | Stream processing engine             | Streaming SQL on Kafka              | Incremental computation engine       |
| **Language**              | C++                                                 | Rust                                        | Java                                      | Rust                                             | Rust                                 | Java                                | Rust                                 |
| **License**               | Apache 2.0                                          | Apache 2.0                                  | Apache 2.0                                | BSL 1.1 (Apache 2.0 after 4 years)               | Apache 2.0 / MIT                     | Confluent Community License         | MIT                                  |
| **Smallest deployment**   | One binary, no dependencies                         | Install script or Docker                    | JobManager + TaskManager JVMs             | Kubernetes + PostgreSQL + object store           | One binary or Docker                 | JVM server + a Kafka cluster        | Docker                               |
| **Scale-out in open source** | No, single node (clusters in Timeplus Enterprise) | Yes                                         | Yes                                       | Community Edition capped at 24 GiB memory        | Yes                                  | Yes, through Kafka                  | Single node in open source           |
| **Where state lives**     | Local disk (stream store + columnar store)          | Object storage (S3) with a local cache      | State backend (RocksDB/ForSt), checkpoints to object storage | Object store + PostgreSQL metadata | Checkpoints to object storage        | Kafka topics + RocksDB              | Memory, spills to local disk         |
| **SQL dialect**           | ClickHouse-style, plus streaming extensions         | PostgreSQL                                  | Flink SQL                                 | PostgreSQL                                       | Arroyo SQL (DataFusion-based)        | KSQL                                | Calcite-based SQL                    |
| **Query results in place** | Yes: streaming and historical queries on streams and materialized views | Yes: materialized views    | No, write to an external sink             | Yes: views and indexes                           | No, write to an external sink        | Limited pull queries                | Yes: views, ad-hoc queries           |
| **Historical analytics**  | Yes, columnar engine derived from ClickHouse        | Point and serving queries; Iceberg scans    | Batch mode, as a separate job             | Not an OLAP engine                               | No                                   | No                                  | Ad-hoc queries on views              |
| **Non-SQL APIs**          | UDFs in JavaScript and Python                       | UDFs in Python, Java, Rust                  | DataStream API, Table API, PyFlink, UDFs  | SQL only                                         | UDFs in Rust and Python              | Java UDFs                           | UDFs in Rust                         |

Sources: [RisingWave README](https://github.com/risingwavelabs/risingwave) and [docs](https://docs.risingwave.com/get-started/intro), [Flink 2.3 release notes](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/), [Materialize README](https://github.com/MaterializeInc/materialize) and [self-managed announcement](https://materialize.com/blog/materialize-for-everyone/), [Arroyo repository](https://github.com/ArroyoSystems/arroyo) and [Cloudflare's acquisition post](https://blog.cloudflare.com/cloudflare-acquires-arroyo-pipelines-streaming-ingestion-beta/), [ksqlDB repository](https://github.com/confluentinc/ksql), [Feldera repository](https://github.com/feldera/feldera).

## What is different about Proton

**It is a streaming engine and an analytics database in one process.** Proton extends the ClickHouse engine with stream processing. Every stream has two stores: an append-only stream store for low-latency reads and a columnar historical store. The same SQL works on both, and you choose with one keyword:

```sql
-- live, unbounded: runs until you cancel it
SELECT device, avg(temperature) FROM sensors GROUP BY device;

-- historical, bounded: returns and exits
SELECT device, avg(temperature) FROM table(sensors) GROUP BY device;
```

A streaming query can also start from history and continue with live data (backfill), which is how you rebuild a materialized view or test a new query against last week's data. Most of the other systems on this page either hold no history (Flink, Arroyo, ksqlDB) or hold materialized results that are tuned for serving, not for scans over billions of raw events.

**It is small.** Proton is a single C++ binary with no JVM, no ZooKeeper, no metadata database and no object store. It starts on 1 vCPU and 0.5 GiB of memory. This is what makes it practical on edge devices, inside CI pipelines, in a Docker Compose file next to your application, and for local development.

**Streaming semantics are first class.** Tumble, hop and session windows, watermarks and late-event handling, several emit policies (after watermark, periodic, on update, changelog), stream-to-stream range joins, ASOF and LATEST joins against versioned streams, changelog processing, and incremental materialized views with checkpointing.

**It speaks to the rest of your stack from SQL.** Kafka, Redpanda, Pulsar and NATS JetStream external streams; external tables for ClickHouse, MySQL, PostgreSQL, MongoDB and S3; Apache Iceberg read and write through a REST catalog; HTTP sinks; and user-defined functions in JavaScript and Python (with an embedded interpreter, no external service).

## Where Proton is not the best choice

- **You need an open-source cluster.** Proton is single node. Clustering, replication and high availability are part of Timeplus Enterprise. RisingWave and Flink scale out under an open-source license.
- **You need elastic, compute-storage separated state.** Proton keeps state on local disk. RisingWave keeps state in object storage so compute nodes are stateless and recover quickly.
- **You need the PostgreSQL dialect.** Proton has a PostgreSQL wire-protocol port, but its SQL dialect and functions follow ClickHouse (in snake_case). Tools that generate PostgreSQL-specific SQL work better with RisingWave or Materialize.
- **You need native database CDC.** Proton reads change data through Debezium and Kafka (see [examples/cdc](examples/cdc)). RisingWave and Materialize connect to PostgreSQL and MySQL replication directly.
- **You need custom operators in code.** Flink's DataStream API has no equivalent in Proton. Proton is SQL plus UDFs.
- **You need strict cross-view transactional consistency.** Materialize and Feldera make this their central guarantee. Proton favors low latency and high throughput with event-time semantics.

## System by system

### RisingWave

[RisingWave](https://github.com/risingwavelabs/risingwave) is the closest project to Proton in ambition: a SQL system that ingests streams, maintains materialized views incrementally, and serves the results. It is written in Rust, licensed under Apache 2.0, and speaks the PostgreSQL wire protocol.

*How they differ.* RisingWave is designed cloud first. State, tables and materialized views live in object storage with a local disk cache, and it is built to run as a distributed cluster. It has native PostgreSQL and MySQL CDC, and it manages Iceberg tables (including a hosted REST catalog and compaction). Proton is designed single-binary first, with local storage and a columnar analytics engine inherited from ClickHouse. That gives Proton fast ad-hoc queries over large raw history and a far wider function library, and gives RisingWave elasticity and simpler recovery at cluster scale.

*Choose RisingWave when* you want an open-source distributed streaming database, PostgreSQL compatibility, native CDC, or state on S3.
*Choose Proton when* you want the smallest possible footprint, heavy historical analytics next to streaming, ClickHouse-style SQL, or edge and embedded deployments.

*A correction.* RisingWave's [March 2026 comparison](https://risingwave.com/blog/risingwave-vs-timeplus-streaming-database/) says Timeplus has no Iceberg sink, supports only JavaScript UDFs, and has limited self-hosting. For current Proton that is not accurate: open-source Proton reads and writes Iceberg (see [examples/iceberg](examples/iceberg)), supports Python UDFs as well as JavaScript, and is fully self-hosted under Apache 2.0.

### Apache Flink

[Flink](https://flink.apache.org/) is the reference stream processing framework. It has the largest connector ecosystem, programmatic APIs in Java and Python, and a long production record at very large scale. Flink 2.x added disaggregated state and materialized tables, and [2.3](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/) continued to round out Flink SQL.

*How they differ.* Flink is a framework you operate: a JobManager, TaskManagers, a state backend, checkpoint storage, and usually a separate database to serve the results. Each SQL statement becomes a job with its own lifecycle. Proton is a database you query: start the binary, write SQL, and the results are stored and queryable in the same place. Flink scales out in open source; Proton does not.

*Choose Flink when* you need custom operators, complex event-driven applications, connectors nobody else has, or scale beyond one machine with open-source software only.
*Choose Proton when* your pipelines are expressible in SQL and you would rather not run a JVM cluster plus a serving database to get them.

### ksqlDB

[ksqlDB](https://github.com/confluentinc/ksql) is streaming SQL on top of Kafka Streams. It needs a Kafka cluster for both data and state, and it is distributed under the Confluent Community License, not an OSI-approved open-source license. Confluent's own streaming SQL investment has moved to Flink, and ksqlDB has seen few new features in recent years.

*How they differ.* ksqlDB reads and writes Kafka only, and pull queries on its tables are limited. Proton reads Kafka without requiring it, joins Kafka data with other sources, stores results in a columnar store, and runs full analytical SQL on them.

*Choose Proton when* you are looking for a ksqlDB replacement that keeps the "SQL on Kafka topics" workflow. `CREATE EXTERNAL STREAM` plus `CREATE MATERIALIZED VIEW ... INTO` covers the usual ksqlDB stream/table patterns.

### Materialize

[Materialize](https://github.com/MaterializeInc/materialize) maintains SQL views incrementally with strong consistency, based on Timely and Differential Dataflow. It speaks PostgreSQL and is strongest on change data from operational databases: complex multi-way joins that always reflect a consistent version of the inputs.

*How they differ.* Materialize is source-available under the BSL 1.1. Self-managed deployments need Kubernetes, a PostgreSQL metadata store and an object store, and the free [Community Edition](https://materialize.com/blog/materialize-for-everyone/) is capped at 24 GiB of memory and 48 GiB of disk. Proton is Apache 2.0 with no size limits, runs without dependencies, and is built for high-volume event streams (logs, metrics, clickstreams, market data) with event-time windows and watermarks, which are not Materialize's focus.

*Choose Materialize when* correctness across views over OLTP data matters more than anything else.
*Choose Proton when* you process high-volume event streams, need time-window semantics, or need a permissive license.

### Arroyo

[Arroyo](https://github.com/ArroyoSystems/arroyo) is a Rust stream processing engine built on Apache Arrow and DataFusion, with a web UI and a single-binary deployment. It was [acquired by Cloudflare in April 2025](https://blog.cloudflare.com/cloudflare-acquires-arroyo-pipelines-streaming-ingestion-beta/) and now powers SQL transformations in Cloudflare Pipelines. The engine remains open source under Apache 2.0 / MIT; the latest tagged release at the time of writing is 0.15 (December 2025).

*How they differ.* Arroyo is a pipeline engine, like Flink: data comes in, is transformed, and goes out to a sink. It does not store results or serve queries. Proton also runs pipelines, but keeps the data and lets you query it. Both are lightweight and SQL first.

*Choose Arroyo when* you want a small Rust pipeline engine with Rust UDFs, or you are building on Cloudflare.
*Choose Proton when* you want to query what you processed, join against history, or backfill.

### Feldera

[Feldera](https://github.com/feldera/feldera) is an incremental computation engine based on the DBSP theory, MIT licensed. Its guarantee is that a view always equals what a batch query would return on the same input, including for recursive queries and deeply nested SQL.

*How they differ.* Feldera's center of gravity is incremental view maintenance over changing tables. Proton's is event streams with time semantics, plus an analytics store. Feldera compiles SQL programs into pipelines; Proton is an interactive database.

*Choose Feldera when* you need exact incremental maintenance of complex SQL, including recursion.
*Choose Proton when* you need windows, watermarks, high-throughput ingestion and interactive analytics.

## About performance

We deliberately do not publish a cross-vendor benchmark table here. Vendor-run comparisons are rarely trusted, and results depend heavily on workload, hardware and tuning. What we can offer:

- Reproducible Proton benchmarks, with the SQL, in [examples/proton3-benchmarks](examples/proton3-benchmarks).
- Proton figures on a laptop (Apple M2 Max): 90 million events per second, 4 ms end-to-end latency, and high-cardinality aggregation with 1 million unique keys. Details are in [this post](https://www.timeplus.com/post/scary-fast).
- An invitation: run your own workload. Proton installs with one command (`curl https://install.timeplus.com/oss | sh`), so a comparison on your data takes minutes to set up.

## Corrections welcome

If you maintain one of the projects above and something here is wrong or out of date, please open an issue or a pull request. We will fix it.
