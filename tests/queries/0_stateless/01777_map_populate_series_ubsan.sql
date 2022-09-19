-- Should correctly throw exception about overflow:
SELECT mapPopulateSeries([-9223372036854775808, to_uint32(2)], [to_uint32(1023), -1]); -- { serverError 128 }
