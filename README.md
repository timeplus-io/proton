[![NightlyTest](https://github.com/timeplus-io/proton/actions/workflows/nightly_test.yml/badge.svg?branch=develop)](https://github.com/timeplus-io/proton/actions/workflows/nightly_test.yml)


---

Proton is a unified streaming and historical data processing engine built on top of ClickHouse code base.

- [What is Proton ?](#what-is-proton)
- [Architecture](#architecture)
- [Key Streaming Functionalities](#key-streaming-functionalities)
- [Docs](#docs)
- [Staring with Timeplus Cloud](#starting-with-timeplus-cloud)
- [Starting with Proton in Container](#starting-with-proton-in-docker-container)
- [License](#license)
- [Contributing](#contributing)
- [Need Help?](#need-help)

## What is Proton?

Proton is a unified streaming and historical data processing engine which powers the Timeplus streaming analytic platform.
It is built on top of trimmed single instance ClickHouse code base. Its major goals are simplicity, efficient with good performance in both streaming and historical query processing.
It is built in one single binary without any external service dependency, so it is easy for users to deploy it on bare metal, edge (ARM), container or in orchestrated cloud environment.
After deployment, users can run streaming queries and historical queries or a combination of both in one SQL. Since its major use cases focus is streaming query processing,
by default, a SQL query in Proton is a streaming query which means it is long-running, never ends and continuously tracks and evaluates the delta changes and push the query results to users or target systems.

## Architecture

The following diagram depicts the high level architecture of single instance Proton. All of the components / functionalities are built into one single binary.
Users can create a stream by using `CREATE STREAM ...` SQL. Every stream has 2 parts at storage layer by default: the real-time streaming data part and the historical data part which
are backed by NativeLog and ClickHouse historical data store respectively. Fundamentally, a stream in Proton is a regular database table with a replicated write-ahead-log in front but is streaming queryable.

When users `INSERT INTO ...` data to Proton, the data always first lands in NativeLog which is immediately queryable. Since NativeLog is in essence a replicated write-ahead-log and is append-only, it
can support high frequent, low latency and large concurrent data ingestion work loads. In background, there is a separate thread tailing the delta data from NativeLog and commits the data in bigger batch
to the historical data store. Since Proton leverages ClickHouse for the historical part, its historical query processing is blazing fast as well.

In quite lots of scenarios, data is already in Kafka / Redpanda or other streaming data hubs, users can create external streams to point to the streaming data hub and do streaming query processing
directly and then either materialize them in Proton or send the query results to external systems.

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

We can run Proton for you and even provide more functionalities in Timeplus console. See our online documentation : Quickstart with [Timeplus Cloud](https://docs.timeplus.com/quickstart).

## Starting with Proton in Docker Container

### Launch Proton Server and Client in Container

After [install Docker engine](https://docs.docker.com/engine/install/) in your OS, pull the latest Proton docker image by running:

```
$ docker pull timeplus/proton:latest
```

Run Proton docker image to run Proton server:

```
$ docker run --name proton timeplus/proton:latest
```


Run Proton client to connect the server:

```
$ docker exec -it proton proton client
```

### Create Stream, Ingest Data and Query

In Proton client console,

```sql
-- Create stream
CREATE STREAM devices(device string, location string, temperature float);

-- Run the stream query and it is always long running waiting for new data
SELECT device, min(temperature), max(temperature) FROM devices GROUP BY device;
```

Launch another Proton client console by running:

```
$ docker exec -it proton proton client
```

Then ingest some data:

```sql
-- Insert some data
INSERT INTO devices (device, location, temperature)
VALUES
('dev1', 'ca', 57.3),
('dev2', 'sh', 37.3),
('dev3', 'van', 17.3);
```

Insert more data and observe the query results.

```sql
INSERT INTO devices (device, location, temperature)
VALUES
('dev1', 'ca', 38.5),
('dev2', 'sh', 18.5),
('dev3', 'van', 88.5);
```

## License

All current code is released under Apache v2 license.

## Contributing

We welcome your contributions! If you are looking for issues to work on, try looking at [the issue list](https://github.com/timeplus-io/proton/issues).

Please see [the wiki](https://github.com/timeplus-io/proton/wiki/Contributing) for more details and see [build from source](BUILD.md) for how to compile Proton in different platforms.

We also encourage users to join [Timeplus Community Slack](https://timeplus.com/slack) and join the dedicated #contributors channel to ask questions.

## Need Help?

- [Timeplus Community Slack](https://timeplus.com/slack) - Join our community slack to connect with our engineers and other users running Proton in #proton channel.
- For filing bugs, suggesting improvements or requesting new features, help us out by [opening an issue](https://github.com/timeplus-io/proton/issues).

