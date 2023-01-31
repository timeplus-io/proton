SELECT '--- Int Empty ---';

SELECT array_reduce('avg_or_default', array_pop_back([1]));
SELECT array_reduce('avg_or_null', array_pop_back([1]));
SELECT array_reduce('stddev_samp_or_default', array_pop_back([1]));
SELECT array_reduce('stddev_samp_or_null', array_pop_back([1]));
SELECT array_reduce('max_or_default', array_pop_back([1]));
SELECT array_reduce('max_or_null', array_pop_back([1]));

SELECT avg_or_default_if(x, x > 1) FROM (SELECT 1 AS x);
SELECT avg_or_null_if(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddev_samp_or_default_if(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddev_samp_or_null_if(x, x > 1) FROM (SELECT 1 AS x);
SELECT max_or_default_if(x, x > 1) FROM (SELECT 1 AS x);
SELECT max_or_null_if(x, x > 1) FROM (SELECT 1 AS x);

SELECT avg_or_default_if_merge(state) FROM (SELECT avg_or_default_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT avg_or_null_if_merge(state) FROM (SELECT avg_or_null_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddev_samp_or_default_if_merge(state) FROM (SELECT stddev_samp_or_default_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddev_samp_or_null_if_merge(state) FROM (SELECT stddev_samp_or_null_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT max_or_default_if_merge(state) FROM (SELECT max_or_default_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT max_or_null_if_merge(state) FROM (SELECT max_or_null_if_state(x, x > 1) AS state FROM (SELECT 1 AS x));

SELECT '--- Int Non-empty ---';

SELECT array_reduce('avg_or_default', [1]);
SELECT array_reduce('avg_or_null', [1]);
SELECT array_reduce('stddev_samp_or_default', [1]);
SELECT array_reduce('stddev_samp_or_null', [1]);
SELECT array_reduce('max_or_default', [1]);
SELECT array_reduce('max_or_null', [1]);

SELECT avg_or_default_if(x, x > 0) FROM (SELECT 1 AS x);
SELECT avg_or_null_if(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddev_samp_or_default_if(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddev_samp_or_null_if(x, x > 0) FROM (SELECT 1 AS x);
SELECT max_or_default_if(x, x > 0) FROM (SELECT 1 AS x);
SELECT max_or_null_if(x, x > 0) FROM (SELECT 1 AS x);

SELECT avg_or_default_if_merge(state) FROM (SELECT avg_or_default_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT avg_or_null_if_merge(state) FROM (SELECT avg_or_null_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddev_samp_or_default_if_merge(state) FROM (SELECT stddev_samp_or_default_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddev_samp_or_null_if_merge(state) FROM (SELECT stddev_samp_or_null_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT max_or_default_if_merge(state) FROM (SELECT max_or_default_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT max_or_null_if_merge(state) FROM (SELECT max_or_null_if_state(x, x > 0) AS state FROM (SELECT 1 AS x));

SELECT '--- Other Types Empty ---';

SELECT array_reduce('max_or_default', array_pop_back(['hello']));
SELECT array_reduce('max_or_null', array_pop_back(['hello']));

SELECT array_reduce('max_or_default', array_pop_back(array_pop_back([to_datetime('2011-04-05 14:19:19'), null])));
SELECT array_reduce('max_or_null', array_pop_back(array_pop_back([to_datetime('2011-04-05 14:19:19'), null])));

SELECT array_reduce('avg_or_default', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('avg_or_null', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('stddev_samp_or_default', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('stddev_samp_or_null', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('max_or_default', array_pop_back([to_decimal128(-123.45, 2)]));
SELECT array_reduce('max_or_null', array_pop_back([to_decimal128(-123.45, 2)]));

SELECT '--- Other Types Non-empty ---';

SELECT array_reduce('max_or_default', ['hello']);
SELECT array_reduce('max_or_null', ['hello']);

SELECT array_reduce('max_or_default', [to_datetime('2011-04-05 14:19:19'), null]);
SELECT array_reduce('max_or_null', [to_datetime('2011-04-05 14:19:19'), null]);

SELECT array_reduce('avg_or_default', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('avg_or_null', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('stddev_samp_or_default', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('stddev_samp_or_null', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('max_or_default', [to_decimal128(-123.45, 2)]);
SELECT array_reduce('max_or_null', [to_decimal128(-123.45, 2)]);
