-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01249;
CREATE DATABASE test_01249 ENGINE=Ordinary;     -- Full ATTACH requires UUID with Atomic
USE test_01249;

create stream bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(0, 1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError 42 }
create stream bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(-0.1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError 36 }
create stream bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(1.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192; -- { serverError 36 }

DROP STREAM IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(0., 1.) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW create stream bloom_filter_idx_good;

DROP STREAM IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(-0.1) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW create stream bloom_filter_idx_good;

DROP STREAM IF EXISTS bloom_filter_idx_good;
ATTACH TABLE bloom_filter_idx_good(`u64` uint64, `i32` int32, `f64` float64, `d` Decimal(10, 2), `s` string, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3), `dt` date, INDEX bloom_filter_a i32 TYPE bloom_filter(1.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192;
SHOW create stream bloom_filter_idx_good;

DROP DATABASE test_01249;
