SELECT toStartOfWeek(to_datetime('1970-01-01 00:00:00', 'UTC'));
SELECT toStartOfWeek(to_datetime('1970-01-01 00:00:00', 'Europe/Moscow'));
SELECT toStartOfWeek(to_datetime('1970-01-01 00:00:00', 'Canada/Atlantic'));
SELECT toStartOfWeek(to_datetime('1970-01-04 00:00:00'));
 

DROP STREAM IF EXISTS t02176;
create stream t02176(timestamp DateTime) ENGINE = MergeTree
PARTITION BY toStartOfWeek(timestamp)
ORDER BY tuple();

INSERT INTO t02176 VALUES (1559952000);

SELECT count() FROM t02176 WHERE timestamp >= to_datetime('1970-01-01 00:00:00');
SELECT count() FROM t02176 WHERE identity(timestamp) >= to_datetime('1970-01-01 00:00:00');

DROP STREAM t02176;
