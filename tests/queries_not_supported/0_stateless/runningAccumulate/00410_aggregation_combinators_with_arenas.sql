DROP STREAM IF EXISTS arena;
create stream arena (k uint8, d string) ;
INSERT INTO arena SELECT number % 10 AS k, hex(int_div(number, 10) % 1000) AS d FROM system.numbers LIMIT 10000000;
SELECT length(groupUniqArrayIf(d, d != hex(0))) FROM arena GROUP BY k;
SELECT length(groupUniqArrayMerge(ds)) FROM (SELECT k, groupUniqArrayState(d) AS ds FROM arena GROUP BY k) GROUP BY k;
DROP STREAM IF EXISTS arena;

SELECT length(arrayReduce('groupUniqArray', [[1, 2], [1],  empty_array_uint8(), [1], [1, 2]]));
SELECT min(x), max(x) FROM (SELECT length(arrayReduce('groupUniqArray', [hex(number), hex(number+1), hex(number)])) AS x FROM system.numbers LIMIT 100000);
SELECT sum(length(runningAccumulate(x))) FROM (SELECT groupUniqArrayState(to_string(number % 10)) AS x, number FROM (SELECT * FROM system.numbers LIMIT 11) GROUP BY number ORDER BY number);
