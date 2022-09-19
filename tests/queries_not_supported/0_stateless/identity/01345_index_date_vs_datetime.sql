DROP STREAM IF EXISTS index;
create stream index (d date) ENGINE = MergeTree ORDER BY d;
INSERT INTO index VALUES ('2020-04-07');
SELECT * FROM index WHERE d > to_datetime('2020-04-06 23:59:59');
SELECT * FROM index WHERE identity(d > to_datetime('2020-04-06 23:59:59'));
DROP STREAM index;
