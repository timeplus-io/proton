SELECT mapPopulateSeries([0xFFFFFFFFFFFFFFFF], [0], 0xFFFFFFFFFFFFFFFF);
SELECT mapPopulateSeries([to_uint64(1)], [1], 0xFFFFFFFFFFFFFFFF); -- { serverError 128 }
