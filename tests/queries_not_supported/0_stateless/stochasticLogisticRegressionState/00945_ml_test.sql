DROP TABLE IF EXISTS defaults;
CREATE TABLE IF NOT EXISTS defaults
(
    param1 float64,
    param2 float64,
    target float64,
    predict1 float64,
    predict2 float64
) ;
insert into defaults(param1,param2,target,predict1,predict2) values (1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2),(1,2,1,-1,-2),(-1,-2,-1,1,2)
DROP STREAM IF EXISTS model;
create stream model engine = Memory as select stochasticLogisticRegressionState(0.1, 0.0, 1.0, 'SGD')(target, param1, param2) as state from defaults;

select ans < 1.1 and ans > 0.9 from
(with (select state from model) as model select evalMLMethod(model, predict1, predict2) as ans from defaults limit 2);

select ans > -0.1 and ans < 0.1 from
(with (select state from model) as model select evalMLMethod(model, predict1, predict2) as ans from defaults limit 2);

DROP TABLE defaults;
DROP TABLE model;
