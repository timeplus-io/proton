DROP STREAM IF EXISTS agg_func_col;

create stream agg_func_col (p date, k uint8, d aggregate_function(sum, uint64) DEFAULT array_reduce('sumState', [to_uint64(200)])) ENGINE = AggregatingMergeTree(p, k, 1);
INSERT INTO agg_func_col (k) VALUES (0);
INSERT INTO agg_func_col (k, d) SELECT 1 AS k, array_reduce('sumState', [to_uint64(100)]) AS d;
SELECT k, sumMerge(d) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER STREAM agg_func_col ADD COLUMN af_avg1 aggregate_function(avg, uint8);
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
INSERT INTO agg_func_col (k, af_avg1) VALUES (2, array_reduce('avgState', [101]));
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER STREAM agg_func_col ADD COLUMN af_gua aggregate_function(groupUniqArray, string) DEFAULT array_reduce('groupUniqArrayState', ['---', '---']);
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
INSERT INTO agg_func_col (k, af_avg1, af_gua) VALUES (3, array_reduce('avgState', [102, 102]), array_reduce('groupUniqArrayState', ['igua', 'igua']));
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

OPTIMIZE STREAM agg_func_col;

SELECT '';
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

DROP STREAM IF EXISTS agg_func_col;

SELECT '';
SELECT array_reduce('groupUniqArrayIf', ['---', '---', 't1'], [1, 1, 0]);
SELECT array_reduce('groupUniqArrayMergeIf',
	[array_reduce('groupUniqArrayState', ['---', '---']), array_reduce('groupUniqArrayState', ['t1', 't'])],
	[1, 0]
);

SELECT '';
SELECT array_reduce('avgState', [0]) IN (array_reduce('avgState', [0, 1]), array_reduce('avgState', [0]));
SELECT array_reduce('avgState', [0]) IN (array_reduce('avgState', [0, 1]), array_reduce('avgState', [1]));

SELECT '';
SELECT array_reduce('uniqExactMerge',
    [array_reduce('uniqExactMergeState',
        [
            array_reduce('uniqExactState', [12345678901]),
            array_reduce('uniqExactState', [12345678901])
        ])
    ]);

SELECT array_reduce('uniqExactMerge',
    [array_reduce('uniqExactMergeState',
        [
            array_reduce('uniqExactState', [12345678901]),
            array_reduce('uniqExactState', [12345678902])
        ])
    ]);
