SET query_mode = 'table';
drop stream if exists table;

create stream table (a uint32,  date date, b uint64,  c uint64, str string, d int8, arr array(uint64), arr_alias array(uint64) ALIAS arr) ENGINE = MergeTree(date, intHash32(c), (a, date, intHash32(c), b), 8192);

SELECT alias2 AS alias3
FROM table 
ARRAY JOIN
    arr_alias AS alias2, 
    array_enumerate_uniq(arr_alias) AS _uniq_Event
WHERE (date = to_date('2010-10-10')) AND (a IN (2, 3)) AND (str NOT IN ('z', 'x')) AND (d != -1)
LIMIT 1;

drop stream if exists table;

