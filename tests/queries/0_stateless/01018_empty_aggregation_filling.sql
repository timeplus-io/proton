SELECT '--- int Empty ---';

SELECT array_reduce('avgOrDefault', array_pop_back([1]));
SELECT array_reduce('avgOrNull', array_pop_back([1]));
SELECT array_reduce('stddevSampOrDefault', array_pop_back([1]));
SELECT array_reduce('stddevSampOrNull', array_pop_back([1]));
SELECT array_reduce('maxOrDefault', array_pop_back([1]));
SELECT array_reduce('maxOrNull', array_pop_back([1]));

SELECT avgOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT avgOrNullIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddevSampOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddevSampOrNullIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT maxOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT maxOrNullIf(x, x > 1) FROM (SELECT 1 AS x);

SELECT avgOrDefaultIfMerge(state) FROM (SELECT avgOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT avgOrNullIfMerge(state) FROM (SELECT avgOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrDefaultIfMerge(state) FROM (SELECT stddevSampOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrNullIfMerge(state) FROM (SELECT stddevSampOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT maxOrDefaultIfMerge(state) FROM (SELECT maxOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT maxOrNullIfMerge(state) FROM (SELECT maxOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));

SELECT '--- int Non-empty ---';

SELECT array_reduce('avgOrDefault', [1]);
SELECT array_reduce('avgOrNull', [1]);
SELECT array_reduce('stddevSampOrDefault', [1]);
SELECT array_reduce('stddevSampOrNull', [1]);
SELECT array_reduce('maxOrDefault', [1]);
SELECT array_reduce('maxOrNull', [1]);

SELECT avgOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT avgOrNullIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddevSampOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddevSampOrNullIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT maxOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT maxOrNullIf(x, x > 0) FROM (SELECT 1 AS x);

SELECT avgOrDefaultIfMerge(state) FROM (SELECT avgOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT avgOrNullIfMerge(state) FROM (SELECT avgOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrDefaultIfMerge(state) FROM (SELECT stddevSampOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrNullIfMerge(state) FROM (SELECT stddevSampOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT maxOrDefaultIfMerge(state) FROM (SELECT maxOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT maxOrNullIfMerge(state) FROM (SELECT maxOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));

SELECT '--- Other Types Empty ---';

SELECT array_reduce('maxOrDefault', array_pop_back(['hello']));
SELECT array_reduce('maxOrNull', array_pop_back(['hello']));

SELECT array_reduce('maxOrDefault', array_pop_back(array_pop_back([to_datetime('2011-04-05 14:19:19'), null])));
SELECT array_reduce('maxOrNull', array_pop_back(array_pop_back([to_datetime('2011-04-05 14:19:19'), null])));

SELECT array_reduce('avgOrDefault', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('avgOrNull', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('stddevSampOrDefault', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('stddevSampOrNull', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('maxOrDefault', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('maxOrNull', array_pop_back([to_decimal128(-123.45, 2)]));

SELECT '--- Other Types Non-empty ---';

SELECT array_reduce('maxOrDefault', ['hello']);
SELECT array_reduce('maxOrNull', ['hello']);

SELECT array_reduce('maxOrDefault', [to_datetime('2011-04-05 14:19:19'), null]);
SELECT array_reduce('maxOrNull', [to_datetime('2011-04-05 14:19:19'), null]);

SELECT array_reduce('avgOrDefault', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('avgOrNull', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('stddevSampOrDefault', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('stddevSampOrNull', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('maxOrDefault', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('maxOrNull', [to_decimal128(-123.45, 2)]);
