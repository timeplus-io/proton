-- Tags: not_supported, blocked_by_SummingMergeTree

DROP STREAM IF EXISTS simple_agf_summing_mt;

create stream simple_agf_summing_mt
(
    a int64,
    min_aggreg aggregate_function(min, uint64),
    min_simple SimpleAggregateFunction(min, uint64),
    max_aggreg aggregate_function(max, uint64),
    max_simple SimpleAggregateFunction(max, uint64),
    sum_aggreg aggregate_function(sum, uint64),
    sum_simple SimpleAggregateFunction(sum, uint64),
    sumov_aggreg aggregate_function(sumWithOverflow, uint64),
    sumov_simple SimpleAggregateFunction(sumWithOverflow, uint64),
    gbitand_aggreg aggregate_function(groupBitAnd, uint64),
    gbitand_simple SimpleAggregateFunction(groupBitAnd, uint64),
    gbitor_aggreg aggregate_function(groupBitOr, uint64),
    gbitor_simple SimpleAggregateFunction(groupBitOr, uint64),
    gbitxor_aggreg aggregate_function(groupBitXor, uint64),
    gbitxor_simple SimpleAggregateFunction(groupBitXor, uint64),
    gra_aggreg aggregate_function(groupArrayArray, array(uint64)),
    gra_simple SimpleAggregateFunction(groupArrayArray, array(uint64)),
    grp_aggreg aggregate_function(groupUniqArrayArray, array(uint64)),
    grp_simple SimpleAggregateFunction(groupUniqArrayArray, array(uint64)),
    aggreg_map aggregate_function(sumMap, tuple(array(string), array(uint64))),
    simple_map SimpleAggregateFunction(sumMap, tuple(array(string), array(uint64))),
    aggreg_map_min aggregate_function(minMap, tuple(array(string), array(uint64))),
    simple_map_min SimpleAggregateFunction(minMap, tuple(array(string), array(uint64))),
    aggreg_map_max aggregate_function(maxMap, tuple(array(string), array(uint64))),
    simple_map_max SimpleAggregateFunction(maxMap, tuple(array(string), array(uint64)))
)
ENGINE = SummingMergeTree
ORDER BY a;

INSERT INTO simple_agf_summing_mt SELECT
    number % 51 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState(number + 111111111),
    groupBitAnd(number + 111111111),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([to_uint64(number % 1000)]),
    groupArrayArray([to_uint64(number % 1000)]),
    groupUniqArrayArrayState([to_uint64(number % 500)]),
    groupUniqArrayArray([to_uint64(number % 500)]),
    sumMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    sumMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

INSERT INTO simple_agf_summing_mt SELECT
    number % 1151 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState(number + 111111111),
    groupBitAnd(number + 111111111),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([to_uint64(number % 1000)]),
    groupArrayArray([to_uint64(number % 1000)]),
    groupUniqArrayArrayState([to_uint64(number % 500)]),
    groupUniqArrayArray([to_uint64(number % 500)]),
    sumMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    sumMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

OPTIMIZE STREAM simple_agf_summing_mt FINAL;

SELECT cityHash64(group_array(cityHash64(*))) FROM (
  SELECT
    a % 31 AS g,
    minMerge(min_aggreg) AS minagg,
    min(min_simple) AS mins,
    minagg = mins AS M,
    maxMerge(max_aggreg) AS maxagg,
    max(max_simple) AS maxs,
    maxagg = maxs AS MX,
    sumMerge(sum_aggreg) AS sumagg,
    sum(sum_simple) AS sums,
    sumagg = sums AS S,
    sumWithOverflowMerge(sumov_aggreg) AS sumaggov,
    sumWithOverflow(sumov_simple) AS sumsov,
    sumaggov = sumsov AS SO,
    groupBitAndMerge(gbitand_aggreg) AS gbitandaggreg,
    groupBitAnd(gbitand_simple) AS gbitandsimple,
    gbitandaggreg = gbitandsimple AS BIT_AND,
    groupBitOrMerge(gbitor_aggreg) AS gbitoraggreg,
    groupBitOr(gbitor_simple) AS gbitorsimple,
    gbitoraggreg = gbitorsimple AS BIT_OR,
    groupBitXorMerge(gbitxor_aggreg) AS gbitxoraggreg,
    groupBitXor(gbitxor_simple) AS gbitxorsimple,
    gbitxoraggreg = gbitxorsimple AS BITXOR,
    arraySort(groupArrayArrayMerge(gra_aggreg)) AS graa,
    arraySort(groupArrayArray(gra_simple)) AS gras,
    graa = gras AS GAA,
    arraySort(groupUniqArrayArrayMerge(grp_aggreg)) AS gra,
    arraySort(groupUniqArrayArray(grp_simple)) AS grs,
    gra = grs AS T,
    sumMapMerge(aggreg_map) AS smmapagg,
    sumMap(simple_map) AS smmaps,
    smmapagg = smmaps AS SM,
    minMapMerge(aggreg_map_min) AS minmapapagg,
    minMap(simple_map_min) AS minmaps,
    minmapapagg = minmaps AS SMIN,
    maxMapMerge(aggreg_map_max) AS maxmapapagg,
    maxMap(simple_map_max) AS maxmaps,
    maxmapapagg = maxmaps AS SMAX
  FROM simple_agf_summing_mt
  GROUP BY g
  ORDER BY g
);

SELECT '---mutation---';

ALTER STREAM simple_agf_summing_mt
    DELETE WHERE (a % 3) = 0
SETTINGS mutations_sync = 1;

INSERT INTO simple_agf_summing_mt SELECT
    number % 11151 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState((number % 3) + 111111110),
    groupBitAnd((number % 3) + 111111110),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([to_uint64(number % 100)]),
    groupArrayArray([to_uint64(number % 100)]),
    groupUniqArrayArrayState([to_uint64(number % 50)]),
    groupUniqArrayArray([to_uint64(number % 50)]),
    sumMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    sumMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    minMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMapState((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))),
    maxMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

OPTIMIZE STREAM simple_agf_summing_mt FINAL;

SELECT cityHash64(group_array(cityHash64(*))) FROM (
  SELECT
    a % 31 AS g,
    minMerge(min_aggreg) AS minagg,
    min(min_simple) AS mins,
    minagg = mins AS M,
    maxMerge(max_aggreg) AS maxagg,
    max(max_simple) AS maxs,
    maxagg = maxs AS MX,
    sumMerge(sum_aggreg) AS sumagg,
    sum(sum_simple) AS sums,
    sumagg = sums AS S,
    sumWithOverflowMerge(sumov_aggreg) AS sumaggov,
    sumWithOverflow(sumov_simple) AS sumsov,
    sumaggov = sumsov AS SO,
    groupBitAndMerge(gbitand_aggreg) AS gbitandaggreg,
    groupBitAnd(gbitand_simple) AS gbitandsimple,
    gbitandaggreg = gbitandsimple AS BIT_AND,
    groupBitOrMerge(gbitor_aggreg) AS gbitoraggreg,
    groupBitOr(gbitor_simple) AS gbitorsimple,
    gbitoraggreg = gbitorsimple AS BIT_OR,
    groupBitXorMerge(gbitxor_aggreg) AS gbitxoraggreg,
    groupBitXor(gbitxor_simple) AS gbitxorsimple,
    gbitxoraggreg = gbitxorsimple AS BITXOR,
    arraySort(groupArrayArrayMerge(gra_aggreg)) AS graa,
    arraySort(groupArrayArray(gra_simple)) AS gras,
    graa = gras AS GAA,
    arraySort(groupUniqArrayArrayMerge(grp_aggreg)) AS gra,
    arraySort(groupUniqArrayArray(grp_simple)) AS grs,
    gra = grs AS T,
    sumMapMerge(aggreg_map) AS smmapagg,
    sumMap(simple_map) AS smmaps,
    smmapagg = smmaps AS SM,
    minMapMerge(aggreg_map_min) AS minmapapagg,
    minMap(simple_map_min) AS minmaps,
    minmapapagg = minmaps AS SMIN,
    maxMapMerge(aggreg_map_max) AS maxmapapagg,
    maxMap(simple_map_max) AS maxmaps,
    maxmapapagg = maxmaps AS SMAX
  FROM simple_agf_summing_mt
  GROUP BY g
  ORDER BY g
);

DROP STREAM simple_agf_summing_mt;
