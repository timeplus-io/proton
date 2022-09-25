SELECT
    array_reduce('uniq', [1, 2, 1]) AS a,
    array_reduce('uniq', [1, 2, 2, 1], ['hello', 'world', '', '']) AS b,
    array_reduce('uniqUpTo(5)', [1, 2, 2, 1], materialize(['hello', 'world', '', ''])) AS c,
    array_reduce('uniqExactIf', [1, 2, 3, 4], [1, 0, 1, 1]) AS d;

SELECT array_reduce('quantiles(0.5, 0.9)', range(number) AS r), r FROM system.numbers LIMIT 12;
