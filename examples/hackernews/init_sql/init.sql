CREATE STREAM IF NOT EXISTS hn_stories_raw(raw string);

CREATE STREAM IF NOT EXISTS hn_comments_raw(raw string);

CREATE MATERIALIZED VIEW IF NOT EXISTS hn_stories AS 
SELECT to_time(raw:time) AS _tp_time,raw:id::int AS id,raw:title AS title,raw:by AS by, raw FROM hn_stories_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS hn_comments AS 
SELECT to_time(raw:time) AS _tp_time,raw:id::int AS id,raw:root_id::int AS root_id,raw:by AS by, raw FROM hn_comments_raw;

CREATE VIEW IF NOT EXISTS story AS SELECT * FROM hn_stories WHERE _tp_time>earliest_ts();

CREATE VIEW IF NOT EXISTS comment AS SELECT * FROM hn_comments WHERE _tp_time>earliest_ts();