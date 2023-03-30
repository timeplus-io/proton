DROP STREAM IF EXISTS t1_00844;
DROP STREAM IF EXISTS t2_00844;

CREATE STREAM IF NOT EXISTS t1_00844 (
f1 uint32,
f2 string
) ENGINE = MergeTree ORDER BY (f1);

CREATE STREAM IF NOT EXISTS t2_00844 (
f1 string,
f3 string
) ENGINE = MergeTree ORDER BY (f1);

insert into t1_00844 values(1,'1');
insert into t2_00844 values('1','name1');

select t1_00844.f1,t2_00844.f3 from t1_00844 all inner join t2_00844 on t1_00844.f2 = t2_00844.f1
where t2_00844.f1 = '1';

DROP STREAM t1_00844;
DROP STREAM t2_00844;
