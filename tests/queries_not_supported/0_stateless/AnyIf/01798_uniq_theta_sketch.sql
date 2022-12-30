-- Tags: no-fasttest, not_supported, blocked_by_SummingMergeTree

SET query_mode = 'table';
SELECT 'uniqTheta many agrs';

SELECT
    uniqTheta(x), uniqTheta((x)), uniqTheta(x, y), uniqTheta((x, y)), uniqTheta(x, y, z), uniqTheta((x, y, z))
FROM
(
    SELECT
        number % 10 AS x,
        int_div(number, 10) % 10 AS y,
        to_string(int_div(number, 100) % 10) AS z
    FROM system.numbers LIMIT 1000
);


SELECT k,
    uniqTheta(x), uniqTheta((x)), uniqTheta(x, y), uniqTheta((x, y)), uniqTheta(x, y, z), uniqTheta((x, y, z)),
    count() AS c
FROM
(
    SELECT
        (number + 0x8ffcbd8257219a26) * 0x66bb3430c06d2353 % 131 AS k,
        number % 10 AS x,
        int_div(number, 10) % 10 AS y,
        to_string(int_div(number, 100) % 10) AS z
    FROM system.numbers LIMIT 100000
)
GROUP BY k
ORDER BY c DESC, k ASC
LIMIT 10;


SELECT 'uniqTheta distinct';

SET count_distinct_implementation = 'uniqTheta';
SELECT count(DISTINCT x) FROM (SELECT number % 123 AS x FROM system.numbers LIMIT 1000);
SELECT count(DISTINCT x, y) FROM (SELECT number % 11 AS x, number % 13 AS y FROM system.numbers LIMIT 1000);


SELECT 'uniqTheta arrays';

SELECT uniqThetaArray([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniqThetaArray([0, 1, 1], [0, 1, 1], [0, 1, 0]);
SELECT uniqTheta(x) FROM (SELECT array_join([[1, 2], [1, 2], [1, 2, 3], []]) AS x);


SELECT 'uniqTheta complex types';

SELECT uniqTheta(x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta(x) FROM (SELECT array_join([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniqTheta(x, x) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta(x, array_map(elem -> [elem, elem], x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta(x, to_string(x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta((x, x)) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta((x, array_map(elem -> [elem, elem], x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta((x, to_string(x))) FROM (SELECT array_join([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqTheta(x) FROM (SELECT array_join([[], ['a'], ['a', NULL, 'b'], []]) AS x);


SELECT 'uniqTheta decimals';

DROP STREAM IF EXISTS decimal;
create stream decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8)
) ;

SELECT (uniqTheta(a), uniqTheta(b), uniqTheta(c))
FROM (SELECT * FROM decimal ORDER BY a);

INSERT INTO decimal (a, b, c)
SELECT to_decimal32(number - 50, 4), to_decimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5
FROM system.numbers LIMIT 101;

SELECT (uniqTheta(a), uniqTheta(b), uniqTheta(c))
FROM (SELECT * FROM decimal ORDER BY a);

DROP STREAM decimal;


SELECT 'uniqTheta remove injective';

set optimize_injective_functions_inside_uniq = 1;

EXPLAIN SYNTAX select uniqTheta(x) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(x + y) from (select number % 2 as x, number % 3 y from numbers(10));
EXPLAIN SYNTAX select uniqTheta(-x) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(bit_not(x)) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(bit_not(-x)) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(-bit_not(-x)) from (select number % 2 as x from numbers(10));

set optimize_injective_functions_inside_uniq = 0;

EXPLAIN SYNTAX select uniqTheta(x) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(x + y) from (select number % 2 as x, number % 3 y from numbers(10));
EXPLAIN SYNTAX select uniqTheta(-x) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(bit_not(x)) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(bit_not(-x)) from (select number % 2 as x from numbers(10));
EXPLAIN SYNTAX select uniqTheta(-bit_not(-x)) from (select number % 2 as x from numbers(10));


DROP STREAM IF EXISTS stored_aggregates;

-- simple
create stream stored_aggregates
(
    d date,
    Uniq aggregate_function(uniq, uint64),
    UniqThetaSketch aggregate_function(uniqTheta, uint64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    to_date('2014-06-01') AS d,
    uniqState(number) AS Uniq,
    uniqThetaState(number) AS UniqThetaSketch
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
);

SELECT uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch) FROM stored_aggregates;

SELECT d, uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch) FROM stored_aggregates GROUP BY d ORDER BY d;

OPTIMIZE STREAM stored_aggregates;

SELECT uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch) FROM stored_aggregates;

SELECT d, uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch) FROM stored_aggregates GROUP BY d ORDER BY d;

DROP STREAM stored_aggregates;

-- complex
create stream stored_aggregates
(
	d	date,
	k1 	uint64,
	k2 	string,
	Uniq 			aggregate_function(uniq, uint64),
    UniqThetaSketch	aggregate_function(uniqTheta, uint64)
)
ENGINE = AggregatingMergeTree(d, (d, k1, k2), 8192);

INSERT INTO stored_aggregates
SELECT
	to_date('2014-06-01') AS d,
	int_div(number, 100) AS k1,
	to_string(int_div(number, 10)) AS k2,
	uniqState(to_uint64(number % 7)) AS Uniq,
    uniqThetaState(to_uint64(number % 7)) AS UniqThetaSketch
FROM
(
	SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1, k2,
	uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1,
	uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d, k1
ORDER BY d, k1;

SELECT d,
	uniq_merge(Uniq), uniqThetaMerge(UniqThetaSketch)
FROM stored_aggregates
GROUP BY d
ORDER BY d;

DROP STREAM stored_aggregates;

---- sum + uniq with more data
drop stream if exists summing_merge_tree_null;
drop stream if exists summing_merge_tree_aggregate_function;
create stream summing_merge_tree_null (
    d materialized today(),
    k uint64,
    c uint64,
    u uint64
) engine=Null;

create materialized view summing_merge_tree_aggregate_function (
    d date,
    k uint64,
    c uint64,
    un aggregate_function(uniq, uint64),
    ut aggregate_function(uniqTheta, uint64)
) engine=SummingMergeTree(d, k, 8192)
as select d, k, sum(c) as c, uniqState(u) as un, uniqThetaState(u) as ut
from summing_merge_tree_null
group by d, k;

-- prime number 53 to avoid resonanse between %3 and %53
insert into summing_merge_tree_null select number % 3, 1, number % 53 from numbers(999999);

select k, sum(c), uniq_merge(un), uniqThetaMerge(ut) from summing_merge_tree_aggregate_function group by k order by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), uniq_merge(un), uniqThetaMerge(ut) from summing_merge_tree_aggregate_function group by k order by k;

drop stream summing_merge_tree_aggregate_function;
drop stream summing_merge_tree_null;

