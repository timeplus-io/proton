DROP STREAM IF EXISTS stored_aggregates;

create stream stored_aggregates
(
	d	date,
	k1 	uint64,
	k2 	string,
	Sum 		aggregate_function(sum, uint64),
	Avg 		aggregate_function(avg, uint64),
	Uniq 		aggregate_function(uniq, uint64),
	Any 		aggregate_function(any, string),
	AnyIf 		aggregate_function(anyIf, string, uint8),
	Quantiles 	aggregate_function(quantiles(0.5, 0.9), uint64),
	GroupArray	aggregate_function(groupArray, string)
)
ENGINE = AggregatingMergeTree(d, (d, k1, k2), 8192);

INSERT INTO stored_aggregates
SELECT
	to_date('2014-06-01') AS d,
	int_div(number, 100) AS k1,
	to_string(int_div(number, 10)) AS k2,
	sumState(number) AS Sum,
	avgState(number) AS Avg,
	uniqState(to_uint64(number % 7)) AS Uniq,
	anyState(to_string(number)) AS Any,
	anyIfState(to_string(number), number % 7 = 0) AS AnyIf,
	quantilesState(0.5, 0.9)(number) AS Quantiles,
	groupArrayState(to_string(number)) AS GroupArray
FROM
(
	SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1, k2,
	sumMerge(Sum), avgMerge(Avg), uniq_merge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d, k1, k2
ORDER BY d, k1, k2;

SELECT d, k1,
	sumMerge(Sum), avgMerge(Avg), uniq_merge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d, k1
ORDER BY d, k1;

SELECT d,
	sumMerge(Sum), avgMerge(Avg), uniq_merge(Uniq),
	anyMerge(Any), anyIfMerge(AnyIf),
	quantilesMerge(0.5, 0.9)(Quantiles),
	groupArrayMerge(GroupArray)
FROM stored_aggregates
GROUP BY d
ORDER BY d;

DROP STREAM stored_aggregates;
