SET query_mode='table';
DROP STREAM IF EXISTS add_aggregate;
create stream add_aggregate(a uint32, b uint32) ;

INSERT INTO add_aggregate(a,b) VALUES(1, 2);
INSERT INTO add_aggregate(a,b) VALUES(3, 1);
select sleep(3);
SELECT count_merge(x + y) FROM (SELECT count_state(a) as x, count_state(b) as y from add_aggregate);
SELECT sum_merge(x + y), sum_merge(x), sum_merge(y) FROM (SELECT sum_state(a) as x, sum_state(b) as y from add_aggregate);
SELECT sum_merge(x) FROM (SELECT sum_state(a) + count_state(b) as x FROM add_aggregate); -- { serverError 421 }
SELECT sum_merge(x) FROM (SELECT sum_state(a) + sum_state(to_int32(b)) as x FROM add_aggregate); -- { serverError 421 }

SELECT min_merge(x) FROM (SELECT min_state(a) + min_state(b) as x FROM add_aggregate); 

SELECT uniq_merge(x + y) FROM (SELECT uniq_state(a) as x, uniq_state(b) as y FROM add_aggregate);

SELECT array_sort(group_array_merge(x + y)) FROM (SELECT group_array_state(a) AS x, group_array_state(b) as y FROM add_aggregate);
SELECT array_sort(group_uniq_array_merge(x + y)) FROM (SELECT groupUniqArrayState(a) AS x, groupUniqArrayState(b) as y FROM add_aggregate);

DROP STREAM IF EXISTS add_aggregate;
