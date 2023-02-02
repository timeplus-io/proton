SELECT finalizeAggregation(*) FROM (select initialize_aggregation('sumMapState', [1, 2], [1, 2], [1, null]));

DROP STREAM IF EXISTS sum_map_overflow;
CREATE STREAM sum_map_overflow(events array(uint8), counts array(uint8)) ENGINE = Log;
SELECT [NULL], sumMapWithOverflow(events, [NULL], [[(NULL)]], counts) FROM sum_map_overflow; -- { serverError 43 }
DROP STREAM sum_map_overflow;
