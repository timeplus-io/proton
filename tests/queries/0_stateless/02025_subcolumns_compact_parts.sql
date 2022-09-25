DROP STREAM IF EXISTS t_comp_subcolumns;

create stream t_comp_subcolumns (id uint32, n nullable(string), arr array(array(uint32)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_comp_subcolumns SELECT number, 'a', [range(number % 11), range(number % 13)] FROM numbers(20000);

SELECT sum(n.null) FROM t_comp_subcolumns;
SELECT n.null FROM t_comp_subcolumns LIMIT 10000, 5;

SELECT sum(arr.size0) FROM t_comp_subcolumns;
SELECT sum_array(arr.size1) FROM t_comp_subcolumns;

DROP STREAM t_comp_subcolumns;
