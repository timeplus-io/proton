CREATE STREAM a (number uint64) ENGINE = MergeTree ORDER BY if(now() > to_datetime('2020-06-01 13:31:40'), to_int64(number), -number); -- { serverError 36 }
CREATE STREAM b (number uint64) ENGINE = MergeTree ORDER BY now() > to_datetime(number); -- { serverError 36 }
CREATE STREAM c (number uint64) ENGINE = MergeTree ORDER BY now(); -- { serverError 36 }
