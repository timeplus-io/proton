# Example to Load Hacker News to Proton via Bytewax

Inspired by https://bytewax.io/blog/polling-hacker-news, you can call Hacker News HTTP API with Bytewax and send latest news to Proton for SQL-based analysis.

## Start the example

Simply run `docker compose up` in this folder. Two docker containers in the stack:

1. ghcr.io/timeplus-io/proton:latest, as the streaming database
2. timeplus/hackernews_bytewax:latest, leveraging [bytewax](https://bytewax.io) to call Hacker News HTTP API with Bytewax and send latest news to Proton. [Source code](https://github.com/timeplus-io/proton-python-driver/tree/develop/example/bytewax)


It will load ~100 items every 15 second and send the data to Proton.

A new stream `hn` will be created automatically, with a single string column `raw`.

You can run SQL to analyze/transform the data, such as

```sql
select raw:id as id, raw:by as by, to_time(raw:time) as time, raw:title as title from hn
```
