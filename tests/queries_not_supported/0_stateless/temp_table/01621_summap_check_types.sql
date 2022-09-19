select initializeAggregation('sumMap', [1, 2], [1, 2], [1, null]);

CREATE TEMPORARY STREAM sum_map_overflow (events array(uint8), counts array(uint8));
INSERT INTO sum_map_overflow VALUES ([1], [255]), ([1], [2]);
SELECT [NULL], sumMapWithOverflow(events, [NULL], [[(NULL)]], counts) FROM sum_map_overflow; -- { serverError 43 }
