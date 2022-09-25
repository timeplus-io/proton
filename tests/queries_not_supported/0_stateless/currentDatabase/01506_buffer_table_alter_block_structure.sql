DROP STREAM IF EXISTS buf_dest;
DROP STREAM IF EXISTS buf;

create stream buf_dest (timestamp datetime)
ENGINE = MergeTree PARTITION BY to_YYYYMMDD(timestamp)
ORDER BY (timestamp);

create stream buf (timestamp datetime) Engine = Buffer(currentDatabase(), buf_dest, 16, 3, 20, 2000000, 20000000, 100000000, 300000000);;

INSERT INTO buf (timestamp) VALUES (to_datetime('2020-01-01 00:05:00'));

ALTER STREAM buf_dest ADD COLUMN s string;
ALTER STREAM buf ADD COLUMN s string;

SELECT * FROM buf;

INSERT INTO buf (timestamp, s) VALUES (to_datetime('2020-01-01 00:06:00'), 'hello');

SELECT * FROM buf ORDER BY timestamp;

DROP STREAM IF EXISTS buf;
DROP STREAM IF EXISTS buf_dest;
