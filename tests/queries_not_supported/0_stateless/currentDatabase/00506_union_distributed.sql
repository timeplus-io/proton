-- Tags: distributed

-- https://github.com/ClickHouse/ClickHouse/issues/1059

SET insert_distributed_sync = 1;

DROP STREAM IF EXISTS union1;
DROP STREAM IF EXISTS union2;
DROP STREAM IF EXISTS union3;

create stream union1 ( date date, a int32, b int32, c int32, d int32) ENGINE = MergeTree(date, (a, date), 8192);
create stream union2 ( date date, a int32, b int32, c int32, d int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'union1');
create stream union3 ( date date, a int32, b int32, c int32, d int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'union2');

INSERT INTO union1 VALUES (1,  2, 3, 4, 5);
INSERT INTO union1 VALUES (11,12,13,14,15);
INSERT INTO union2 VALUES (21,22,23,24,25);
INSERT INTO union3 VALUES (31,32,33,34,35);

select b, sum(c) from ( select a, b, sum(c) as c from union2 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from union2 where b>1 group by a, b  order by a, b) as a group by b order by b;
select b, sum(c) from ( select a, b, sum(c) as c from union1 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from union2 where b>1 group by a, b order by a, b) as a group by b order by b;
select b, sum(c) from ( select a, b, sum(c) as c from union1 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from union1 where b>1 group by a, b order by a, b) as a group by b order by b;
select b, sum(c) from ( select a, b, sum(c) as c from union2 where a>1 group by a,b UNION ALL select a, b, sum(c) as c from union3 where b>1 group by a, b order by a, b) as a group by b order by b;

DROP STREAM union1;
DROP STREAM union2;
DROP STREAM union3;
