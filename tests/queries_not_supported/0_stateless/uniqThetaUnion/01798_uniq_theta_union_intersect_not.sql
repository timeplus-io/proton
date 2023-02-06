-- Tags: no-fasttest

SELECT 'uniqTheta union test';

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[]) as a, array_reduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[1,2]) as a, array_reduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaUnion(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[2,3,4]) as a, array_reduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta intersect test';

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[]) as a, array_reduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[1,2]) as a, array_reduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaIntersect(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[2,3,4]) as a, array_reduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta union test';

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[]) as a, array_reduce('uniqThetaState',[]) as b );

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[1,2]) as a, array_reduce('uniqThetaState',[2,3,4]) as b );

select finalizeAggregation(uniqThetaNot(a, b)), finalizeAggregation(a), finalizeAggregation(b) from (select array_reduce('uniqThetaState',[2,3,4]) as a, array_reduce('uniqThetaState',[1,2]) as b );

SELECT 'uniqTheta retention test';

select finalizeAggregation(uniqThetaIntersect(a,b)), finalizeAggregation(a),finalizeAggregation(b) from 
(
select (uniqThetaStateIf(number, number>0)) as a, (uniqThetaStateIf(number, number>5)) as b 
from 
(select  number  FROM system.numbers LIMIT 10)
);

SELECT 'uniqTheta retention with AggregatingMergeTree test';
DROP STREAM IF EXISTS test1;

CREATE STREAM test1
(
    `year` string ,
    `uv` aggregate_function(uniqTheta, int64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (year);

INSERT INTO STREAM test1(year, uv) select '2021',uniqThetaState(to_int64(1));
INSERT INTO STREAM test1(year, uv) select '2021',uniqThetaState(to_int64(2));
INSERT INTO STREAM test1(year, uv) select '2021',uniqThetaState(to_int64(3));
INSERT INTO STREAM test1(year, uv) select '2021',uniqThetaState(to_int64(4));
INSERT INTO STREAM test1(year, uv) select '2022',uniqThetaState(to_int64(1));
INSERT INTO STREAM test1(year, uv) select '2022',uniqThetaState(to_int64(3));

select finalizeAggregation(uniqThetaIntersect(uv2021,uv2022))/finalizeAggregation(uv2021),finalizeAggregation(uniqThetaIntersect(uv2021,uv2022)),finalizeAggregation(uv2021)
from
(
select uniqThetaMergeStateIf(uv,year='2021') as uv2021, uniqThetaMergeStateIf(uv,year='2022') as uv2022 
from test1
);

DROP STREAM IF EXISTS test1;

SELECT 'uniqTheta retention with MergeTree test';
DROP STREAM IF EXISTS test2;

CREATE STREAM test2
(
    `year` string ,
    `uv`  int64
)
ENGINE = MergeTree()
ORDER BY (year);

INSERT INTO STREAM test2(year, uv) select '2021',1;
INSERT INTO STREAM test2(year, uv) select '2021',2;
INSERT INTO STREAM test2(year, uv) select '2021',3;
INSERT INTO STREAM test2(year, uv) select '2021',4;
INSERT INTO STREAM test2(year, uv) select '2022',1;
INSERT INTO STREAM test2(year, uv) select '2022',3;

select finalizeAggregation(uniqThetaIntersect(uv2021,uv2022))/finalizeAggregation(uv2021),finalizeAggregation(uniqThetaIntersect(uv2021,uv2022)),finalizeAggregation(uv2021)
from
(
select uniqThetaStateIf(uv,year='2021') as uv2021, uniqThetaStateIf(uv,year='2022') as uv2022 
from test2
);



DROP STREAM IF EXISTS test2;
