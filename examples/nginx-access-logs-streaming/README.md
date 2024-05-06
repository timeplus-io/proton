# Analyzing Nginx Access Logs using Timeplus Proton 
In a [recent blog post](https://www.timeplus.com/post/log-stream-analysis), we walked through three different ways of using Timeplus Proton to ingest and analyse a log file in real-time.

I’ll explore a similar theme in the first part of this post: ad-hoc analysis of web traffic, in real-time, using Timeplus Proton. I will use a Node.js blog that is behind an Nginx web server for the analysis. 

In the second part of this post, I will analyze past traffic stats for the blog. I'll compare historical traffic stats obtained using Timeplus Proton with traffic stats reported by [Umami](https://umami.is). (Umami is an open source, privacy-focused alternative to Google Analytics.) 

The numbers from Timeplus Proton should be of higher accuracy than the numbers reported by Umami because Umami, like Google Analytics, is a JavaScript-based analytics product and thus susceptible to under-reporting traffic from users with ad blocking enabled.

## Background
The blog that we will using is my personal blog which I launched back in 2020. It's a [Ghost](https://ghost.org/) blog hosted on AWS. Ghost is written in JavaScript/Node.js and is completely [open source](https://github.com/tryghost/ghost).

In production, the Ghost blog is deployed behind Nginx which serves as a reverse proxy, so the Ghost blog is in fact two services: 
* a Node.js web app that listens on port `2368`
* an Nginx web server that listens for incoming traffic on the standard HTTP/HTTPS ports `80` and `443`. It handles SSL termination for the Node.js app. 

The Nginx access logs are regularly rotated so I have multiple rotated log files dating back to 4 years ago when the blog was first launched in `/var/log/nginx/`.

<details>
<summary>Nginx logs directory listing.</summary>
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

In addition to log file rotation, Nginx uses compression to keep the storage requirements of log files inside `/var/log/nginx/` to a minimum. After about 4 years, the `/var/log/nginx/` weighs in at just `2.7MB`:
```bash
du -shL /var/log/nginx/
2.7M	/var/log/nginx/
```

The Node.js web app for Ghost on the other hand writes its logs to `/var/www/ghost/content/logs/` and that folder is already approaching `700MB` on disk: 
```bash
du -shL /var/www/ghost/content/logs/
692M  /var/www/ghost/content/logs/
```
To keep things simple, I'll only make use of the access logs for Nginx in this post.


## Introducing a New Contender for SQL-based Observability
[SQL-based Observability](https://clickhouse.com/blog/the-state-of-sql-based-observability#is-sql-based-observability-applicable-to-my-use-case) is steadily growing in popularity as an alternative to the [ELK stack](https://aws.amazon.com/what-is/elk-stack/) and the major tool at the center of this trend is ClickHouse due to its blazing-fast log-handling features. 

Two metrics that help ClickHouse stand out relative to alternatives are:
* ingestion speed and
* query speed.

For instance, Uber's Log Analytics platform, which used to be based on ELK, could only handle [~25.5k docs per second](https://www.elastic.co/blog/data-ingestion-elasticsearch) compared to an ingestion speed of [300K logs per second](https://www.uber.com/en-PT/blog/logging/) on a single ClickHouse node.

More than 80% of their queries are aggregation queries but ELK was not designed to support fast aggregations across large datasets. This lead to very slow query speeds for aggregation queries over a 1-hour window (on a 1.3TB dataset) and frequent time outs for aggregations over a 6-hour window. 

ClickHouse's columnar design allows it to support fast aggregations across large datasets out-of-the-box. The Uber team were able to further speedup the execution time of aggregation queries on ClickHouse by *materializing frequently queried fields into their own columns*. The historical analysis in the second part of this blog will make use of this technique.

Timeplus Proton extends the already excellent log-handling features of ClickHouse with streaming making it a perfect candidate for our first and second tasks: real-time traffic analysis and historical traffic analysis. 


# Real-time Analysis of Web Traffic
## Ingesting Logs using only Timeplus Proton
The recent Timeplus blog which I mentioned earlier: [Real-Time Log Stream Analysis Using an Open-Source Streaming Database](https://www.timeplus.com/post/log-stream-analysis) goes over 3 stacks for streaming logs to Timeplus Proton for analysis:

1. Filebeat + Kafka + Proton
2. Vector + Proton
3. Proton

![Filebeat + Kafka + Proton](https://static.wixstatic.com/media/2d747e_1873fec48d79407f828c42ae5c94333f~mv2.png/v1/fill/w_1480,h_260,al_c,q_90,usm_0.66_1.00_0.01,enc_auto/2d747e_1873fec48d79407f828c42ae5c94333f~mv2.png)
*Stack 1: Filebeat + Kafka + Proton*

![Vector + Proton](https://static.wixstatic.com/media/2d747e_1d01a2a6b0074a999bc8e3c091a26162~mv2.png/v1/fill/w_1036,h_458,al_c,q_90,enc_auto/2d747e_1d01a2a6b0074a999bc8e3c091a26162~mv2.png)
*Stack 2: Vector + Proton*

![Proton](https://static.wixstatic.com/media/2d747e_773438fadf4b4fb89f449294d928616d~mv2.png/v1/fill/w_1391,h_338,al_c,lg_1,q_90,enc_auto/2d747e_773438fadf4b4fb89f449294d928616d~mv2.png)
*Stack 3: Proton Only*

Out of the three stacks, the simplest to setup is the 3rd stack where only Timeplus Proton can be used for the log analysis and this is what will be used in this post.

There's a caveat: the simpler architecture has an important limitation which is that real-time log analysis can add significant load to a server, if the server is under-resourced. 
It can significantly degrade the performance of the server on which it is run and might not scale well for real-world use.

To avoid the performance and scalability limitations of the selected stack, since the blog itself is already hosted on AWS, I'm going to perform the analysis using a separate EC2 instance. 

I will install Timeplus Proton on a separate EC2 instance and make the `/var/log/nginx/` folder available over a readonly NFS share. In other words, the Ghost blog and Timeplus proton EC2 instances will be inside the same VPC subnet making it easy to securely share the access logs between the two instances over a readonly NFS share.
 
This approach of using NFS (instead of [Amazon EFS](https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html)) allows us to keep the costs of the solution down since there will be no egress fees while also being secure.

## Sharing the Nginx Access Log over NFS
The private IP addresses of the 2 EC2 instances I will be using for the NFS share are captured in the table below:
| EC2 Server Instance | Private IP |
|--------------|------------|
| Ghost web blog  | `172.31.17.58` |
| Timeplus Proton | `172.31.24.29` |


### NFS Server Setup
1. SSH into the server for the Ghost web blog.

2. Install NFS server components:
```bash
sudo apt-get install nfs-kernel-server -y
```

3. Create the directory to be shared:
```bash
sudo mkdir -p /var/log/nginx
```

4. Configure the NFS export such that only the Timeplus Proton server can access it by whitelisting its (private) IP address:
```bash
echo "/var/log/nginx  172.31.24.29(ro,sync,no_subtree_check)" | sudo tee -a /etc/exports
```

5. Export the shared directory:
```bash
sudo exportfs -ra
```

6. Start the NFS Server on the Ghost blog:
```bash
sudo systemctl restart nfs-kernel-server
```

7. Confirm that the NFS export is active:
```bash
showmount -e
Export list for ip-172-31-17-58:
/var/log/nginx 172.31.24.29
```

8. Don't forget to add an inbound rule for NFS (on port `2049`) in the security group for the Ghost blog instance.


### NFS Client Setup
1. SSH into the server for Timeplus Proton.

2. Install NFS client components:
```bash
sudo add-apt-repository universe -y && sudo apt update -y 
sudo apt install nfs-common -y
```

3. Create the mount point where the NFS share will be mounted inside the Timeplus Proton server:
```bash
sudo mkdir -p /mnt/nginx
```

4. Mount the NFS share:
```bash
sudo mount 172.31.17.58:/var/log/nginx /mnt/nginx
```

5. List the folder contents to confirm the log files were mounted:
```bash
cd /mnt/nginx
ls -lh
```

6. Install Timeplus Proton:
```bash
curl https://install.timeplus.com | sh
```

7. Start the Timeplus Proton server:
```bash
./proton server
```

8. Start Timeplus Proton client:
```bash
./proton client --host 127.0.0.1
```

9. Create a stream for real-time monitoring of the log files:
```sql
CREATE EXTERNAL STREAM nginx_access_log (
  raw string
)
SETTINGS
  type='log',
  log_files='access.log',
  log_dir='/mnt/nginx',
  timestamp_regex='(\[\d{2}\/\w+\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\])',
  row_delimiter='(\n)'
```

10. Running this query should return results. It should continuously return results each time live traffic hits the blog:
```sql
select * from nginx_access_log;
```

![Nginx access log streaming demo](timeplus-proton-nginx-streaming-logs7.gif)


# Historical Analysis of Web Traffic
TBD.

## Script
<details>
<summary>Output of the Nginx access log to CSV conversion script.</summary>
<pre>
./csv.sh 
Deleting old csv files.
removed '/mnt/csv/nginx/access.ipinfo.csv'
removed '/mnt/csv/nginx/access.log.csv'
'/var/log/nginx/access.log' -> '/mnt/csv/nginx/access.log.0'
'/var/log/nginx/access.log.1' -> '/mnt/csv/nginx/access.log.1'
'/var/log/nginx/access.log.10.gz' -> '/mnt/csv/nginx/access.log.10.gz'
'/var/log/nginx/access.log.11.gz' -> '/mnt/csv/nginx/access.log.11.gz'
'/var/log/nginx/access.log.12.gz' -> '/mnt/csv/nginx/access.log.12.gz'
'/var/log/nginx/access.log.13.gz' -> '/mnt/csv/nginx/access.log.13.gz'
'/var/log/nginx/access.log.14.gz' -> '/mnt/csv/nginx/access.log.14.gz'
'/var/log/nginx/access.log.2.gz' -> '/mnt/csv/nginx/access.log.2.gz'
'/var/log/nginx/access.log.3.gz' -> '/mnt/csv/nginx/access.log.3.gz'
'/var/log/nginx/access.log.4.gz' -> '/mnt/csv/nginx/access.log.4.gz'
'/var/log/nginx/access.log.5.gz' -> '/mnt/csv/nginx/access.log.5.gz'
'/var/log/nginx/access.log.6.gz' -> '/mnt/csv/nginx/access.log.6.gz'
'/var/log/nginx/access.log.7.gz' -> '/mnt/csv/nginx/access.log.7.gz'
'/var/log/nginx/access.log.8.gz' -> '/mnt/csv/nginx/access.log.8.gz'
'/var/log/nginx/access.log.9.gz' -> '/mnt/csv/nginx/access.log.9.gz'
Decompressing gzipped /mnt/csv/nginx/access.log.10.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.11.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.12.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.13.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.14.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.2.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.3.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.4.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.5.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.6.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.7.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.8.gz.
Decompressing gzipped /mnt/csv/nginx/access.log.9.gz.
[Python] Converting [access.log.0] to [access.log.0.csv]: (valid => 3604, malicious => 26). Total rows processed: 3630.
removed '/mnt/csv/nginx/access.log.0'
[Python] Converting [access.log.1] to [access.log.1.csv]: (valid => 3446, malicious => 28). Total rows processed: 3474.
removed '/mnt/csv/nginx/access.log.1'
[Python] Converting [access.log.10] to [access.log.10.csv]: (valid => 7916, malicious => 45). Total rows processed: 7961.
removed '/mnt/csv/nginx/access.log.10'
[Python] Converting [access.log.11] to [access.log.11.csv]: (valid => 3373, malicious => 84). Total rows processed: 3457.
removed '/mnt/csv/nginx/access.log.11'
[Python] Converting [access.log.12] to [access.log.12.csv]: (valid => 3349, malicious => 45). Total rows processed: 3394.
removed '/mnt/csv/nginx/access.log.12'
[Python] Converting [access.log.13] to [access.log.13.csv]: (valid => 8361, malicious => 30). Total rows processed: 8391.
removed '/mnt/csv/nginx/access.log.13'
[Python] Converting [access.log.14] to [access.log.14.csv]: (valid => 7332, malicious => 45). Total rows processed: 7377.
removed '/mnt/csv/nginx/access.log.14'
[Python] Converting [access.log.2] to [access.log.2.csv]: (valid => 5009, malicious => 48). Total rows processed: 5057.
removed '/mnt/csv/nginx/access.log.2'
[Python] Converting [access.log.3] to [access.log.3.csv]: (valid => 2720, malicious => 42). Total rows processed: 2762.
removed '/mnt/csv/nginx/access.log.3'
[Python] Converting [access.log.4] to [access.log.4.csv]: (valid => 7240, malicious => 32). Total rows processed: 7272.
removed '/mnt/csv/nginx/access.log.4'
[Python] Converting [access.log.5] to [access.log.5.csv]: (valid => 2760, malicious => 58). Total rows processed: 2818.
removed '/mnt/csv/nginx/access.log.5'
[Python] Converting [access.log.6] to [access.log.6.csv]: (valid => 4209, malicious => 42). Total rows processed: 4251.
removed '/mnt/csv/nginx/access.log.6'
[Python] Converting [access.log.7] to [access.log.7.csv]: (valid => 2464, malicious => 43). Total rows processed: 2507.
removed '/mnt/csv/nginx/access.log.7'
[Python] Converting [access.log.8] to [access.log.8.csv]: (valid => 2409, malicious => 43). Total rows processed: 2452.
removed '/mnt/csv/nginx/access.log.8'
[Python] Converting [access.log.9] to [access.log.9.csv]: (valid => 2947, malicious => 57). Total rows processed: 3004.
removed '/mnt/csv/nginx/access.log.9'
Removing CSV headers from /mnt/csv/nginx/access.log.1.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.10.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.11.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.12.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.13.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.14.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.2.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.3.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.4.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.5.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.6.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.7.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.8.csv.
Removing CSV headers from /mnt/csv/nginx/access.log.9.csv.
Combined all access logs into a single file: /mnt/csv/nginx/access.log.csv.
removed '/mnt/csv/nginx/access.log.0.csv'
removed '/mnt/csv/nginx/access.log.1.csv'
removed '/mnt/csv/nginx/access.log.2.csv'
removed '/mnt/csv/nginx/access.log.3.csv'
removed '/mnt/csv/nginx/access.log.4.csv'
removed '/mnt/csv/nginx/access.log.5.csv'
removed '/mnt/csv/nginx/access.log.6.csv'
removed '/mnt/csv/nginx/access.log.7.csv'
removed '/mnt/csv/nginx/access.log.8.csv'
removed '/mnt/csv/nginx/access.log.9.csv'
removed '/mnt/csv/nginx/access.log.10.csv'
removed '/mnt/csv/nginx/access.log.11.csv'
removed '/mnt/csv/nginx/access.log.12.csv'
removed '/mnt/csv/nginx/access.log.13.csv'
removed '/mnt/csv/nginx/access.log.14.csv'
Extracting only IP addresses from /mnt/csv/nginx/access.log.csv to /mnt/csv/nginx/access.ipinfo.tmp ...
Removing duplicate IP addresses from /mnt/csv/nginx/access.ipinfo.tmp ...
Combined all IP addresses into a single file: /mnt/csv/nginx/access.ipinfo.
removed '/mnt/csv/nginx/access.ipinfo.tmp'
Geo-locating all the IP addresses in bulk using the IPInfo API (https://ipinfo.io):
   Total IP addresses that will be looked up in bulk using /mnt/csv/nginx/access.ipinfo: 3037.
   Geo-lookup of 3037 IP addresses written to file: /mnt/csv/nginx/access.ipinfo.csv.
   Complete!
removed '/mnt/csv/nginx/access.ipinfo'
</pre>
</details>
