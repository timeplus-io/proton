-- regression for "DB::Exception: Size of filter doesn't match size of column.."
SELECT to_datetime(fromUnixTimestamp64Micro(to_int64(0)), 'UTC') as ts FROM numbers_mt(2) WHERE ts + 1 = ts;

-- regression for "Invalid number of rows in Chunk column uint32: expected 2, got 1."
SELECT to_datetime(fromUnixTimestamp64Micro(to_int64(0)), 'UTC') ts FROM numbers(2);
