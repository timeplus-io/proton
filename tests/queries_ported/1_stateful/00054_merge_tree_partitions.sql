DROP STREAM IF EXISTS test.partitions;
CREATE STREAM test.partitions (EventDate date, CounterID uint32) ENGINE = MergeTree(EventDate, CounterID, 8192);
INSERT INTO test.partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 1704509;
select sleep(3);

SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= to_date('2015-01-01') AND EventDate < to_date('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < to_date('2015-01-01') OR EventDate >= to_date('2015-02-01');

ALTER STREAM test.partitions DETACH PARTITION 201501;

SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= to_date('2015-01-01') AND EventDate < to_date('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < to_date('2015-01-01') OR EventDate >= to_date('2015-02-01');

ALTER STREAM test.partitions ATTACH PARTITION 201501;

SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= to_date('2015-01-01') AND EventDate < to_date('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < to_date('2015-01-01') OR EventDate >= to_date('2015-02-01');


ALTER STREAM test.partitions DETACH PARTITION 201403;

SELECT count() FROM test.partitions;

INSERT INTO test.partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 1704509 AND to_start_of_month(EventDate) = to_date('2014-03-01');
select sleep(3);

SELECT count() FROM test.partitions;

ALTER STREAM test.partitions ATTACH PARTITION 201403;

SELECT count() FROM test.partitions;


DROP STREAM test.partitions;
