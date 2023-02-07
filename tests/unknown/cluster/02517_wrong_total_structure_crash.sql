-- Tags: no-ordinary-database
CREATE OR REPLACE STREAM alias10__fuzz_13 (`Id` array(array(uint256)), `EventDate` array(string), `field1` array(array(nullable(int8))), `field2` array(Date), `field3` array(array(array(uint128)))) ENGINE = Distributed(test_shard_localhost, current_database(), alias_local10);


CREATE OR REPLACE STREAM alias_local10 (
  Id int8,
  EventDate Date DEFAULT '2000-01-01',
  field1 int8,
  field2 string,
  field3 ALIAS CASE WHEN field1 = 1 THEN field2 ELSE '0' END
) ENGINE = MergeTree(EventDate, (Id, EventDate), 8192);

SET prefer_localhost_replica = 0;

SELECT field1 FROM alias10__fuzz_13 WHERE arrayEnumerateDense(NULL, tuple('0.2147483646'), NULL) GROUP BY field1, arrayEnumerateDense(('0.02', '0.1', '0'), NULL) WITH TOTALS; -- { serverError TYPE_MISMATCH }


CREATE OR REPLACE STREAM local (x int8) ENGINE = Memory;
CREATE OR REPLACE STREAM distributed (x array(int8)) ENGINE = Distributed(test_shard_localhost, current_database(), local);
SET prefer_localhost_replica = 0;
SELECT x FROM distributed GROUP BY x WITH TOTALS; -- { serverError TYPE_MISMATCH }

DROP STREAM distributed;
DROP STREAM local;
DROP STREAM alias_local10;
DROP STREAM alias10__fuzz_13;
