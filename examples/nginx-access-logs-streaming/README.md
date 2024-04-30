# Analyzing Nginx Access Logs using Timeplus Proton 
In a [recent article](https://www.timeplus.com/post/log-stream-analysis), we walked through three different ways by which Timeplus Proton could be used to ingest and analyse its own log files in real-time.

In the first part of this article I’ll explore a similar theme: use Timeplus Proton to analyze web traffic in real-time. I will use a Node.js blog that is behind an Nginx web server for the analysis. 

In the second part of this article I will analyze past traffic numbers for the blog. I'll compare historical traffic numbers obtained using Timeplus Proton with traffic numbers reported by [Umami](https://umami.is). (Umami is an open source, privacy-focused alternative to Google Analytics.) 

The numbers from Timeplus Proton should be of higher accuracy than the numbers reported by Umami because Umami, like Google Analytics, is a JavaScript-based analytics product and thus susceptible to under-reporting traffic from users with ad blocking enabled.

# Real-time Analysis of Nginx Access Logs
The steady rise of SQL-based Observability makes this super easy to setup.


# Historical Analysis of Nginx Access Logs


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
