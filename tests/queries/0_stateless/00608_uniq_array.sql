SELECT uniq(x) FROM (SELECT array_join([[1, 2], [1, 2], [1, 2, 3], []]) AS x);
SELECT uniqExact(x) FROM (SELECT array_join([[1, 2], [1, 2], [1, 2, 3], []]) AS x);
SELECT uniqUpTo(2)(x) FROM (SELECT array_join([[1, 2], [1, 2], [1, 2, 3], []]) AS x);
