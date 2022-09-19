SELECT finalize_aggregation(*) FROM (select initializeAggregation('sumMapState', [1, 2], [1, 2], [1, null]));

DROP STREAM IF EXISTS sum_map_overflow;
create stream sum_map_overflow(events array(uint8), counts array(uint8))  ;
SELECT [NULL], sumMapWithOverflow(events, [NULL], [[(NULL)]], counts) FROM sum_map_overflow; -- { serverError 43 }
DROP STREAM sum_map_overflow;
