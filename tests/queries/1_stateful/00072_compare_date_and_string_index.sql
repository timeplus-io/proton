SELECT count() FROM test.hits WHERE EventDate = '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate < '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate > '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate <= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate >= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate IN ('2014-03-18', '2014-03-19');

SELECT count() FROM test.hits WHERE EventDate = to_date('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate < to_date('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate > to_date('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate <= to_date('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate >= to_date('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate IN (to_date('2014-03-18'), to_date('2014-03-19'));

SELECT count() FROM test.hits WHERE EventDate = concat('2014-0', '3-18');

DROP STREAM IF EXISTS test.hits_indexed_by_time;
CREATE TABLE test.hits_indexed_by_time (EventDate date, EventTime DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY (EventDate, EventTime);
INSERT INTO test.hits_indexed_by_time SELECT EventDate, EventTime FROM test.hits;

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime < '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime > '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime <= '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime >= '2014-03-18 01:02:03';
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime IN ('2014-03-18 01:02:03', '2014-03-19 04:05:06');

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = to_datetime('2014-03-18 01:02:03', 'Europe/Moscow');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime < to_datetime('2014-03-18 01:02:03', 'Europe/Moscow');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime > to_datetime('2014-03-18 01:02:03', 'Europe/Moscow');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime <= to_datetime('2014-03-18 01:02:03', 'Europe/Moscow');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime >= to_datetime('2014-03-18 01:02:03', 'Europe/Moscow');
SELECT count() FROM test.hits_indexed_by_time WHERE EventTime IN (to_datetime('2014-03-18 01:02:03', 'Europe/Moscow'), to_datetime('2014-03-19 04:05:06', 'Europe/Moscow'));

SELECT count() FROM test.hits_indexed_by_time WHERE EventTime = concat('2014-03-18 ', '01:02:03');

DROP STREAM test.hits_indexed_by_time;
