CREATE STREAM test_s64_local (date Date, value int64) ENGINE = MergeTree order by tuple();
CREATE STREAM test_u64_local (date Date, value uint64) ENGINE = MergeTree order by tuple();
CREATE STREAM test_s64_distributed AS test_s64_local ENGINE = Distributed('test_shard_localhost', current_database(), test_s64_local, rand());
CREATE STREAM test_u64_distributed AS test_u64_local ENGINE = Distributed('test_shard_localhost', current_database(), test_u64_local, rand());

SELECT * FROM merge(current_database(), '') WHERE value = 1048575;
