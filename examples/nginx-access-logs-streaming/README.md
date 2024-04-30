# Analyzing Nginx Access Logs using Timeplus Proton 
In a [recent article](https://www.timeplus.com/post/log-stream-analysis), we walked through three different ways of using Timeplus Proton to ingest and analyse a log file in real-time.

I’ll explore a similar theme in the first part of this article: use Timeplus Proton for ad-hoc analysis of web traffic, in real-time. I will use a Node.js blog that is behind an Nginx web server for the analysis. 

In the second part of this article, I will analyze past traffic numbers for the blog. I'll compare historical traffic numbers obtained using Timeplus Proton with traffic numbers reported by [Umami](https://umami.is). (Umami is an open source, privacy-focused alternative to Google Analytics.) 

The numbers from Timeplus Proton should be of higher accuracy than the numbers reported by Umami because Umami, like Google Analytics, is a JavaScript-based analytics product and thus susceptible to under-reporting traffic from users with ad blocking enabled.

## Background
The blog that we will analyse is has two logs.


## Introducing a New Contender for SQL-based Observability
[SQL-based Observability](https://clickhouse.com/blog/the-state-of-sql-based-observability) is steadily growing in popularity as an alternative to the [ELK stack](https://aws.amazon.com/what-is/elk-stack/) and the major tool at the center of this trend is ClickHouse due to its blazing-fast log-handling features. 

Two metrics that help ClickHouse stand out relative to alternatives are:
* ingestion speed and;
* query speed.

For instance, Uber's Log Analytics platform, which used to be based on ELK, could only handle up to [~25.5k docs per second](https://www.elastic.co/blog/data-ingestion-elasticsearch) compared to an ingestion speed of [up to 300K logs per second](https://www.uber.com/en-PT/blog/logging/) on a single ClickHouse node.

More than 80% of their queries are aggregation queries but ELK was not designed to support fast aggregations across large datasets. This lead to very slow query speeds for aggregation queries over a 1-hour window (on a 1.3TB dataset) and frequent time outs for aggregations over a 6-hour window. 

ClickHouse's columnar design allows it to support fast aggregations across large datasets out-of-the-box. The Uber team were able to further speedup the execution time of aggregation queries on ClickHouse by materializing frequently queried fields into their own columns.

Timeplus Proton extends the already excellent log-handling features of ClickHouse with streaming making it a perfect candidate for our first and second tasks: real-time traffic analysis and historical traffic analysis. 


# Real-time Analysis of Web Traffic
The steady rise of SQL-based Observability makes this super easy to setup.


# Historical Analysis of Web Traffic


## SQL-based Observability
SQL-based Observability is gradually making in-roads With observability.




## todo
- [ ] setup a VPC
- [ ] start another EC2 instance inside the VPC running Proton
- [ ] ship historical logs to Proton
- [ ] ship live logs to Proton
  - [ ] share the log file using Amazon EFS?
- [ ] analysis
  - [ ] stats on malformed requests: 404, 403, 401
  - [ ] review nginx error logs too ?
  - [ ] compare Umami analytics with raw access logs to account for ad-blocker traffic
