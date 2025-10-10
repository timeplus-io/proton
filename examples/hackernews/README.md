# Example to Load Hacker News to Proton via Bytewax

Inspired by https://bytewax.io/blog/polling-hacker-news, you can call Hacker News HTTP API with Bytewax and send latest news to Proton for SQL-based analysis.

## Start the example

Simply run `docker compose up` in this folder and it will start
1. d.timeplus.com/timeplus-io/proton:latest, with pre-configured streams, materialized views and views.
2. timeplus/hackernews_bytewax:latest, leveraging [bytewax](https://bytewax.io) to call Hacker News HTTP API with Bytewax and send latest news to Proton. [Source code](https://github.com/timeplus-io/proton-python-driver/tree/develop/example/bytewax)
3. A pre-configured Grafana instance to visulaize the live data.


It will start 5 bytewax workers and load 150 items on startup, then load new items every 15 seconds and send the data to Proton.

## How it works

When the Proton server is started, we create 2 streams to receive the raw JSON data pushed from Bytewax.
```sql
CREATE STREAM hn_stories_raw(raw string);
CREATE STREAM hn_comments_raw(raw string);
```
Then we create 2 materialized view to extract the key information from the JSON and put into more meaningful columns:
```sql
CREATE MATERIALIZED VIEW hn_stories AS
  SELECT to_time(raw:time) AS _tp_time,raw:id::int AS id,raw:title AS title,raw:by AS by, raw FROM hn_stories_raw;
CREATE MATERIALIZED VIEW hn_comments AS
  SELECT to_time(raw:time) AS _tp_time,raw:id::int AS id,raw:root_id::int AS root_id,raw:by AS by, raw FROM hn_comments_raw;
```
Finally we create 2 views to load both incoming data and existin data:
```sql
CREATE VIEW IF NOT EXISTS story AS SELECT * FROM hn_stories WHERE _tp_time>earliest_ts();
CREATE VIEW IF NOT EXISTS comment AS SELECT * FROM hn_comments WHERE _tp_time>earliest_ts()
```

With all those streams and views, you can query the data in whatever ways, e.g.
```sql
select * from comment;

select
    story._tp_time as story_time,comment._tp_time as comment_time,
    story.id as story_id, comment.id as comment_id,
    substring(story.title,1,20) as title,substring(comment.raw:text,1,20) as comment
from story join comment on story.id=comment.root_id;
```

### Querying and visualizing with Grafana

Please try the docker-compose file. The Grafana instance is setup to install [Proton Grafana Data Source Plugin](https://github.com/timeplus-io/proton-grafana-source). Create such a data source and preconfigure a dashboard. Open Grafana UI at http://localhost:3000 in your browser and choose the `Hackernews Live Dashboard`.
