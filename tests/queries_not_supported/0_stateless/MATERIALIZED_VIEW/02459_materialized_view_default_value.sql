DROP STREAM IF EXISTS session;
DROP STREAM IF EXISTS queue;
DROP STREAM IF EXISTS forward;

CREATE STREAM session
(
    `day` Date,
    `uid` string,
    `dummy` string DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (day, uid);

CREATE STREAM queue
(
    `day` Date,
    `uid` string
)
ENGINE = MergeTree
ORDER BY (day, uid);

CREATE MATERIALIZED VIEW IF NOT EXISTS forward INTO session AS
SELECT
    day,
    uid
FROM queue;

insert into queue values ('2019-05-01', 'test');

SELECT * FROM queue;
SELECT * FROM session;
SELECT * FROM forward;

DROP STREAM session;
DROP STREAM queue;
DROP STREAM forward;
