DROP STREAM IF EXISTS buf_dest;
DROP STREAM IF EXISTS buf;

create stream buf_dest (timestamp DateTime)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp);

create stream buf (timestamp DateTime) Engine = Buffer(currentDatabase(), buf_dest, 16, 0.1, 0.1, 2000000, 20000000, 100000000, 300000000);;

INSERT INTO buf (timestamp) VALUES (to_datetime('2020-01-01 00:05:00'));

--- wait for buffer to flush
SELECT sleep(1) from numbers(1) settings max_block_size=1 format Null;

ALTER STREAM buf_dest ADD COLUMN s string;
ALTER STREAM buf ADD COLUMN s string;

SELECT * FROM buf;

INSERT INTO buf (timestamp, s) VALUES (to_datetime('2020-01-01 00:06:00'), 'hello');

SELECT * FROM buf ORDER BY timestamp;

DROP STREAM IF EXISTS buf;
DROP STREAM IF EXISTS buf_dest;
