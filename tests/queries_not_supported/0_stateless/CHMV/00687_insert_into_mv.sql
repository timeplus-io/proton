DROP STREAM IF EXISTS test_00687;
DROP STREAM IF EXISTS mv_bad;
DROP STREAM IF EXISTS mv_good;
DROP STREAM IF EXISTS mv_group;

create stream test_00687 (x string) ENGINE = Null;

create MATERIALIZED VIEW mv_bad (x string)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT DISTINCT x FROM test_00687;

create MATERIALIZED VIEW mv_good (x string)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test_00687;

create MATERIALIZED VIEW mv_group (x string)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test_00687 group by x;

insert into test_00687 values ('stest'), ('stest');

select * from mv_bad;
SELECT '---';
select * from mv_good;
SELECT '---';
select * from mv_group;

DROP STREAM mv_bad;
DROP STREAM mv_good;
DROP STREAM mv_group;
DROP STREAM test_00687;
