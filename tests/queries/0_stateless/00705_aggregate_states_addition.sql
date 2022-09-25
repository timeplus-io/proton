 
DROP STREAM IF EXISTS add_aggregate;
create stream add_aggregate(a uint32, b uint32) ;

INSERT INTO add_aggregate VALUES(1, 2);
INSERT INTO add_aggregate VALUES(3, 1);

SELECT countMerge(x + y) FROM (SELECT countState(a) as x, countState(b) as y from add_aggregate);
SELECT sumMerge(x + y), sumMerge(x), sumMerge(y) FROM (SELECT sumState(a) as x, sumState(b) as y from add_aggregate);
SELECT sumMerge(x) FROM (SELECT sumState(a) + countState(b) as x FROM add_aggregate); -- { serverError 421 }
SELECT sumMerge(x) FROM (SELECT sumState(a) + sumState(to_int32(b)) as x FROM add_aggregate); -- { serverError 421 }

SELECT minMerge(x) FROM (SELECT minState(a) + minState(b) as x FROM add_aggregate); 

SELECT uniqMerge(x + y) FROM (SELECT uniq_state(a) as x, uniq_state(b) as y FROM add_aggregate);

SELECT arraySort(groupArrayMerge(x + y)) FROM (SELECT groupArrayState(a) AS x, groupArrayState(b) as y FROM add_aggregate);
SELECT arraySort(groupUniqArrayMerge(x + y)) FROM (SELECT groupUniqArrayState(a) AS x, groupUniqArrayState(b) as y FROM add_aggregate);

DROP STREAM IF EXISTS add_aggregate;
