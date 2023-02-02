SELECT map_populate_series([0xFFFFFFFFFFFFFFFF], [0], 0xFFFFFFFFFFFFFFFF);
SELECT map_populate_series([to_uint64(1)], [1], 0xFFFFFFFFFFFFFFFF); -- { serverError 128 }
