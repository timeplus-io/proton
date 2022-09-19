-- Tags: shard

SELECT X FROM (SELECT * FROM (SELECT 1 AS X, 2 AS Y) UNION ALL SELECT 3, 4) ORDER BY X;

DROP STREAM IF EXISTS globalin;

create stream globalin (CounterID uint32, StartDate date ) ;

INSERT INTO globalin VALUES (34, to_date('2017-10-02')), (42, to_date('2017-10-02')), (55, to_date('2017-10-01'));

SELECT * FROM ( SELECT CounterID FROM remote('127.0.0.2', currentDatabase(), 'globalin') WHERE (CounterID GLOBAL IN ( SELECT to_uint32(34))) GROUP BY CounterID);
SELECT 'NOW okay =========================:';
SELECT CounterID FROM remote('127.0.0.2', currentDatabase(), 'globalin') WHERE (CounterID GLOBAL IN ( SELECT to_uint32(34) )) GROUP BY CounterID  UNION ALL SELECT CounterID FROM remote('127.0.0.2', currentDatabase(), 'globalin') WHERE (CounterID GLOBAL IN ( SELECT to_uint32(34))) GROUP BY CounterID;
SELECT 'NOW BAD ==========================:';
SELECT * FROM ( SELECT CounterID FROM remote('127.0.0.2', currentDatabase(), 'globalin') WHERE (CounterID GLOBAL IN ( SELECT to_uint32(34) )) GROUP BY CounterID  UNION ALL SELECT CounterID FROM remote('127.0.0.2', currentDatabase(), 'globalin') WHERE (CounterID GLOBAL IN ( SELECT to_uint32(34))) GROUP BY CounterID);
SELECT 'finish ===========================;';

DROP STREAM globalin;


DROP STREAM IF EXISTS union_bug;

create stream union_bug (
    Event string,
    Datetime DateTime('Europe/Moscow')
) Engine = Memory;

INSERT INTO union_bug VALUES ('A', 1), ('B', 2);

SELECT ' * A UNION * B:';
SELECT * FROM (
  SELECT * FROM union_bug WHERE Event = 'A'
 UNION ALL
  SELECT * FROM union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' Event, Datetime A UNION * B:';
SELECT * FROM (
  SELECT Event, Datetime FROM union_bug WHERE Event = 'A'
 UNION ALL
  SELECT * FROM union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' * A UNION Event, Datetime B:';
SELECT * FROM (
  SELECT * FROM union_bug WHERE Event = 'A'
 UNION ALL
  SELECT Event, Datetime FROM union_bug WHERE Event = 'B'
) ORDER BY Datetime;

SELECT ' Event, Datetime A UNION Event, Datetime B:';
SELECT * FROM (
  SELECT Event, Datetime FROM union_bug WHERE Event = 'A'
 UNION ALL
  SELECT Event, Datetime FROM union_bug WHERE Event = 'B'
) ORDER BY Datetime;


DROP STREAM union_bug;
