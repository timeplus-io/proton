<p align="center">
  <img alt="Proton – open source, unified streaming and data processing engine for real-time analytics" src="design/proton-logo-white-bg.png"
  />
</p>
<p align="center">
  <a href="https://github.com/timeplus-io/proton/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/timeplus-io/proton?label=license&logo=github&color=blue" alt="License" /></a>
  <a href="https://github.com/timeplus-io/proton/"><img src="https://img.shields.io/github/stars/timeplus-io/proton?logo=github" /></a>
  <a href="https://timeplus.com/slack"><img src="https://img.shields.io/badge/Join%20Slack-blue?logo=slack" alt="Slack" /></a>
  <a href="https://linkedin.com/company/timeplusinc"><img src="https://img.shields.io/badge/timeplusinc-0077B5?style=social&logo=linkedin" alt="follow on LinkedIn"></a>
  <a href="https://www.youtube.com/@timeplusdata"><img src="https://img.shields.io/youtube/channel/views/UCRQCOw9wOiqHZkm7ftAMdTQ" alt="YouTube" /></a>
  <a href="https://twitter.com/intent/follow?screen_name=timeplusdata"><img src="https://img.shields.io/twitter/follow/timeplusdata" alt="Twitter(X)" /></a>
</p>


<p align="center">
  <a href="#introduction"><strong>Introduction</strong></a> ·
  <a href="#architecture"><strong>Architecture</strong></a> ·
  <a href="#get-started"><strong>Get Started</strong></a> ·
  <a href="#whats-next"><strong>What's next</strong></a> ·
  <a href="#documentation"><strong>Documentation</strong></a> ·
  <a href="#contributing"><strong>Contributing</strong></a> ·
  <a href="#need-help"><strong>Need help?</strong></a>
</p>

## Introduction

Proton is a unified streaming and historical data processing engine in a single binary. It helps data engineers and platform engineers solve complex real-time analytics use cases, and powers the [Timeplus](https://timeplus.com) streaming analytics platform.

Proton extends the historical data, storage, and computing functionality of the popular [ClickHouse project](https://github.com/clickhouse/clickhouse) with streaming and OLAP data processing.

Why use Proton?

- **A unified, lightweight engine** to connect streaming and historical data processing tasks with efficiency and robust performance.
- **A smooth developer experience** with powerful streaming and analytical functionality.
- **Flexible deployments** with Proton's single binary and no external service dependencies.
- **Low total cost of ownership** compared to other analytical frameworks.

Plus built-in support for powerful streaming and analytical functionality:

| Functionality                                                | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| <b>[Data transformation](https://docs.timeplus.com/usecases#data)</b> | Scrub sensitive fields, derive new columns from raw data, or convert identifiers to human-readable information. |
| <b>[Joining streams](https://docs.timeplus.com/joins)</b>    | Combine data from different sources to add freshness to the resulting stream. |
| <b>[Aggregating streams](https://docs.timeplus.com/functions_for_agg)</b> | Developer-friendly functions to derive insights from streaming and historical data. |
| <b>Windowed stream processing ([tumble](https://docs.timeplus.com/functions_for_streaming#tumble) / [hop](https://docs.timeplus.com/functions_for_streaming#hop) / [session](https://docs.timeplus.com/functions_for_streaming#session))</b> | Collect insightful snapshots of streaming data.              |
| <b>[Substreams](https://docs.timeplus.com/substream)</b>     | Maintain separate watermarks and streaming windows.          |
| <b>[Data revision processing (changelog)](https://docs.timeplus.com/changelog-stream)</b> | Create and manage non-append streams with primary keys and change data capture (CDC) semantics. |
| <b>[Federated streaming queries](https://docs.timeplus.com/external-stream)</b> | Query streaming data in external systems (e.g. Kafka) without duplicating them. |
| <b>[Materialized views](https://docs.timeplus.com/view#m_view)</b> | Create long-running and internally-stored queries.           |

## Architecture

![Architecture](design/proton-high-level-arch.svg)

See our [architecture](https://docs.timeplus.com/proton-architecture) doc for technical details and the [FAQ](https://docs.timeplus.com/proton-faq) for more information on the various editions of Proton, how it's related to ClickHouse, and why we chose Apache License 2.0.

## Get started

With [Docker engine](https://docs.docker.com/engine/install/) installed on your local machine, pull and run the latest version of the Proton Docker image.

(For Mac or Linux users, you can also download the [single binary](https://github.com/timeplus-io/proton/wiki/Install-single-binary-Proton) or use [homebrew](https://github.com/timeplus-io/homebrew-timeplus) to use Proton without Docker.)

```bash
docker run -d --pull always --name proton ghcr.io/timeplus-io/proton:latest
```

Connect to your `proton` container and run the `proton-client` tool to connect to the local Proton server:

```bash
docker exec -it proton proton-client -n
```

If you stop the container and want to start it again, run `docker start proton`.

### Query a test stream

From `proton-client`, run the following SQL to create a stream of random data:

```sql
-- Create a stream with random data.
CREATE RANDOM STREAM devices(device string default 'device'||to_string(rand()%4), temperature float default rand()%1000/10);

-- Run the long-running stream query.
SELECT device, count(*), min(temperature), max(temperature) FROM devices GROUP BY device;
```

You should see data like the following:

```
┌─device──┬─count()─┬─min(temperature)─┬─max(temperature)─┐
│ device0 │    2256 │                0 │             99.6 │
│ device1 │    2260 │              0.1 │             99.7 │
│ device3 │    2259 │              0.3 │             99.9 │
│ device2 │    2225 │              0.2 │             99.8 │
└─────────┴─────────┴──────────────────┴──────────────────┘
```

### What's next?

Now that you're running Proton and have created your first stream, query, and view, you can explore [reading and writing data from Apache Kafka](https://docs.timeplus.com/proton-kafka#tutorial) with External Streams, or view the [Proton documentation](https://docs.timeplus.com/proton) to explore additional capabilities.

To see more examples of using Proton, check out the [examples](https://github.com/timeplus-io/proton/tree/develop/examples) folder.

The following drivers are available:

* https://github.com/timeplus-io/proton-java-driver JDBC and other Java clients
* https://github.com/timeplus-io/proton-go-driver
* https://github.com/timeplus-io/proton-python-driver

Integrations with other systems:

* Grafana https://github.com/timeplus-io/proton-grafana-source
* Metabase  https://github.com/timeplus-io/metabase-proton-driver
* Pulse UI https://github.com/timeplus-io/pulseui/tree/proton
* Homebrew https://github.com/timeplus-io/homebrew-timeplus
* dbt https://github.com/timeplus-io/dbt-proton

## Get more with Timeplus

To access more features, such as sources, sinks, dashboards, alerts, data lineage, create a workspace at [Timeplus Cloud](https://us.timeplus.cloud) or try the [live demo](https://demo.timeplus.cloud) with pre-built live data and dashboards.

## Documentation

We publish full documentation for Proton at [docs.timeplus.com](https://docs.timeplus.com/proton) alongside documentation for the Timeplus (Cloud and Enterprise) platform.

We also have a [FAQ](https://docs.timeplus.com/proton-faq/) for detailing how we chose Apache License 2.0, how Proton is related to ClickHouse, what features are available in Proton versus Timeplus, and more.


## Contributing

We welcome your contributions! If you are looking for issues to work on, try looking at [the issue list](https://github.com/timeplus-io/proton/issues).

Please see the [wiki](https://github.com/timeplus-io/proton/wiki/Contributing) for more details, and [BUILD.md](https://github.com/timeplus-io/proton/blob/develop/BUILD.md) to compile Proton in different platforms.

We also encourage you to join the [Timeplus Community Slack](https://timeplus.com/slack) to ask questions and meet other active contributors from Timeplus and beyond.

## Need help?

Join the [Timeplus Community Slack](https://timeplus.com/slack) to connect with Timeplus engineers and other Proton
users.

For filing bugs, suggesting improvements, or requesting new features, see the [open issues](https://github.com/timeplus-io/proton/issues) here on GitHub.

## Licensing

Proton uses Apache License 2.0. See details in the [LICENSE](https://github.com/timeplus-io/proton/blob/develop/LICENSE).


