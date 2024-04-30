# Analyzing Nginx Access Logs using Timeplus Proton 
In a [recent article](https://www.timeplus.com/post/log-stream-analysis), we walked through three different ways of using Timeplus Proton to ingest and analyse a log file in real-time.

I’ll explore a similar theme in the first part of this article: use Timeplus Proton to analyze web traffic in real-time. I will use a Node.js blog that is behind an Nginx web server for the analysis. 

In the second part of this article, I will analyze past traffic numbers for the blog. I'll compare historical traffic numbers obtained using Timeplus Proton with traffic numbers reported by [Umami](https://umami.is). (Umami is an open source, privacy-focused alternative to Google Analytics.) 

The numbers from Timeplus Proton should be of higher accuracy than the numbers reported by Umami because Umami, like Google Analytics, is a JavaScript-based analytics product and thus susceptible to under-reporting traffic from users with ad blocking enabled.

## Introduction
ClickHouse is steadily growing in popularity as an alternative to the [ELK](https://aws.amazon.com/what-is/elk-stack/) stack because it is part of a larger trend of [SQL-based Observability](https://clickhouse.com/blog/the-state-of-sql-based-observability). Timeplus Proton extends the already excellent ClickHouse with streaming capabilities making it a perfect candidate for our first goal: log-handling in real-time and second goal: ad-hoc analysis. 

The blog that we will analyse is has two logs.


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
