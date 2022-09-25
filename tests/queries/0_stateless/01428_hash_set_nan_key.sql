SELECT uniq_exact(nan) FROM numbers(1000);
SELECT uniq_exact(number + nan) FROM numbers(1000);
SELECT sumDistinct(number + nan) FROM numbers(1000);
SELECT DISTINCT number + nan FROM numbers(1000);

SELECT topKWeightedMerge(1)(initializeAggregation('topKWeightedState(1)', nan, array_join(range(10))));

select number + nan k from numbers(256) group by k;

SELECT uniq_exact(reinterpret_as_float64(reinterpretAsFixedString(reinterpretAsUInt64(reinterpretAsFixedString(nan)) + number))) FROM numbers(10);
