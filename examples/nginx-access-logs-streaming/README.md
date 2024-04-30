# Analyzing Nginx Access Logs using Timeplus Proton 
In a [recent article](https://www.timeplus.com/post/log-stream-analysis), we walked through three different ways of using Timeplus Proton to ingest and analyse a log file in real-time.

I’ll explore a similar theme in the first part of this article: ad-hoc analysis of web traffic, in real-time, using Timeplus Proton. I will use a Node.js blog that is behind an Nginx web server for the analysis. 

In the second part of this article, I will analyze past traffic numbers for the blog. I'll compare historical traffic numbers obtained using Timeplus Proton with traffic numbers reported by [Umami](https://umami.is). (Umami is an open source, privacy-focused alternative to Google Analytics.) 

The numbers from Timeplus Proton should be of higher accuracy than the numbers reported by Umami because Umami, like Google Analytics, is a JavaScript-based analytics product and thus susceptible to under-reporting traffic from users with ad blocking enabled.

## Background
The blog that we will using is my personal blog which I launched back in 2020. It's a [Ghost](https://ghost.org/) blog hosted on AWS. Ghost is written in JavaScript/Node.js and is completely [open source](https://github.com/tryghost/ghost).

In production, the Ghost blog is deployed behind nginx which serves as a reverse proxy, so the Ghost blog is in fact two services: 
* a Node.js web app that listens on port `2368`
* an nginx web server that listens for incoming traffic on the standard HTTP/HTTPS ports `80` and `443`. It handles SSL termination for the Node.js app. 

The nginx access logs are regularly rotated so I have multiple rotated log files dating back to 4 years ago when the blog was first launched in `/var/log/nginx/`.

<details>
<summary>nginx logs directory listing.</summary>
<pre>
ll /var/log/nginx/
total 2152
drwxr-xr-x  3 root     adm      4096 Apr 30 00:00 ./
drwxrwxr-x 13 root     syslog   4096 Apr 28 00:00 ../
drwxr-xr-x  2 www-data adm      4096 Apr 28 16:42 2020-10_2023-06/
-rw-r-----  1 www-data adm    418174 Apr 30 22:36 access.log
-rw-r-----  1 www-data adm    799201 Apr 29 23:59 access.log.1
-rw-r-----  1 www-data adm     47009 Apr 20 23:59 access.log.10.gz
-rw-r-----  1 www-data adm     49434 Apr 19 23:59 access.log.11.gz
-rw-r-----  1 www-data adm     44373 Apr 19 00:00 access.log.12.gz
-rw-r-----  1 www-data adm    109735 Apr 17 23:59 access.log.13.gz
-rw-r-----  1 www-data adm     64309 Apr 17 00:00 access.log.14.gz
-rw-r-----  1 www-data adm     41989 Apr 28 23:59 access.log.2.gz
-rw-r-----  1 www-data adm     40176 Apr 27 23:59 access.log.3.gz
-rw-r-----  1 www-data adm     51326 Apr 26 23:59 access.log.4.gz
-rw-r-----  1 www-data adm     96012 Apr 26 00:00 access.log.5.gz
-rw-r-----  1 www-data adm     57779 Apr 24 23:59 access.log.6.gz
-rw-r-----  1 www-data adm     54235 Apr 23 23:59 access.log.7.gz
-rw-r-----  1 www-data adm     91835 Apr 22 23:59 access.log.8.gz
-rw-r-----  1 www-data adm     89026 Apr 22 00:00 access.log.9.gz
-rw-r-----  1 www-data adm      4080 Apr 30 00:28 error.log
-rw-r-----  1 www-data adm     33801 Apr 29 23:35 error.log.1
-rw-r-----  1 www-data adm       221 Apr 18 16:02 error.log.10.gz
-rw-r-----  1 www-data adm       535 Apr 17 21:11 error.log.11.gz
-rw-r-----  1 www-data adm       224 Apr 16 23:01 error.log.12.gz
-rw-r-----  1 www-data adm       227 Apr 15 06:32 error.log.13.gz
-rw-r-----  1 www-data adm       331 Apr 14 15:44 error.log.14.gz
-rw-r-----  1 www-data adm       313 Apr 28 05:53 error.log.2.gz
-rw-r-----  1 www-data adm       274 Apr 27 11:49 error.log.3.gz
-rw-r-----  1 www-data adm       319 Apr 26 09:45 error.log.4.gz
-rw-r-----  1 www-data adm       279 Apr 25 13:30 error.log.5.gz
-rw-r-----  1 www-data adm       350 Apr 23 22:43 error.log.6.gz
-rw-r-----  1 www-data adm       291 Apr 22 22:27 error.log.7.gz
-rw-r-----  1 www-data adm       265 Apr 21 05:55 error.log.8.gz
-rw-r-----  1 www-data adm       292 Apr 19 21:49 error.log.9.gz
</pre>
</details>

In addition to log file rotation, nginx uses compression to keep the storage requirements of log files inside `/var/log/nginx/` to a minimum. After about 4 years, the `/var/log/nginx/` weighs in at just `2.7MB`:
```bash
du -shL /var/log/nginx/
2.7M	/var/log/nginx/
```

The Node.js web app for Ghost on the other hand writes its logs to `/var/www/ghost/content/logs/` and that folder is already approaching `700MB` on disk: 
```bash
du -shL /var/www/ghost/content/logs/
692M  /var/www/ghost/content/logs/
```
To keep things simple, I'll only make use of the access logs for nginx in the article.


## Introducing a New Contender for SQL-based Observability
[SQL-based Observability](https://clickhouse.com/blog/the-state-of-sql-based-observability) is steadily growing in popularity as an alternative to the [ELK stack](https://aws.amazon.com/what-is/elk-stack/) and the major tool at the center of this trend is ClickHouse due to its blazing-fast log-handling features. 

Two metrics that help ClickHouse stand out relative to alternatives are:
* ingestion speed and;
* query speed.

For instance, Uber's Log Analytics platform, which used to be based on ELK, could only handle up to [~25.5k docs per second](https://www.elastic.co/blog/data-ingestion-elasticsearch) compared to an ingestion speed of [up to 300K logs per second](https://www.uber.com/en-PT/blog/logging/) on a single ClickHouse node.

More than 80% of their queries are aggregation queries but ELK was not designed to support fast aggregations across large datasets. This lead to very slow query speeds for aggregation queries over a 1-hour window (on a 1.3TB dataset) and frequent time outs for aggregations over a 6-hour window. 

ClickHouse's columnar design allows it to support fast aggregations across large datasets out-of-the-box. The Uber team were able to further speedup the execution time of aggregation queries on ClickHouse by *materializing frequently queried fields into their own columns*. The historical analysis in the second part of this article will make use of this technique.

Timeplus Proton extends the already excellent log-handling features of ClickHouse with streaming making it a perfect candidate for our first and second tasks: real-time traffic analysis and historical traffic analysis. 


# Real-time Analysis of Web Traffic
## Ingesting Logs using only Timeplus Proton
The recent Timeplus article which I mentioned earlier: [Real-Time Log Stream Analysis Using an Open-Source Streaming Database](https://www.timeplus.com/post/log-stream-analysis) goes over 3 ways to stream logs to Timeplus Proton for analysis:

* Filebeat + Kafka + Proton
* Vector + Proton 
* Proton 

Out of the three options, the simplest to setup is the 3rd option where only Timeplus Proton was used for the log analysis and this is what will be used in this article.

There's a caveat: the simpler architecture has an important limitation which is that real-time log analysis can add significant load to a server, if the server is under-resourced. 
It can significantly degrade the performance of the server on which it is run and might not scale well for real-world use.

To avoid the performance and scalability limitations of the 3rd option, since the blog itself is already hosted on AWS, I'm going to perform the analysis using a separate EC2 instance. 

I will install Timeplus Proton on a separate EC2 instance and make the `/var/log/nginx/` folder available over a readonly NFS share. In other words, the Ghost blog and Timeplus proton EC2 instances will be inside the same VPC subnet making it easy to securely share the access logs between the two instances over a readonly NFS share.
 
This approach of using NFS (instead of [Amazon EFS](https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html)) allows us to keep the costs of the solution down (since there will be no egress fees) while also being secure.

## Setting up NFS
TBD.


# Historical Analysis of Web Traffic
TBD.

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
