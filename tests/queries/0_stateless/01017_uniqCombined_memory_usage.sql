-- Tags: no-tsan, no-asan, no-msan, no-replicated-database
-- Tag no-tsan: Fine thresholds on memory usage
-- Tag no-asan: Fine thresholds on memory usage
-- Tag no-msan: Fine thresholds on memory usage

-- each uniqCombined state should not use > sizeof(HLL) in memory,
-- sizeof(HLL) is (2^K * 6 / 8)
-- hence max_memory_usage for 100 rows = (96<<10)*100 = 9830400

-- HashTable for uint32 (used until (1<<13) elements), hence 8192 elements
SELECT 'uint32';
SET max_memory_usage = 4000000;
SELECT sum(u) FROM (SELECT int_div(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 9830400;
SELECT sum(u) FROM (SELECT int_div(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k);

-- HashTable for uint64 (used until (1<<12) elements), hence 4096 elements
SELECT 'uint64';
SET max_memory_usage = 4000000;
SELECT sum(u) FROM (SELECT int_div(number, 4096) AS k, uniqCombined(reinterpret_as_string(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 9830400;
SELECT sum(u) FROM (SELECT int_div(number, 4096) AS k, uniqCombined(reinterpret_as_string(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k);

SELECT 'K=16';

-- HashTable for uint32 (used until (1<<12) elements), hence 4096 elements
SELECT 'uint32';
SET max_memory_usage = 2000000;
SELECT sum(u) FROM (SELECT int_div(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 4915200;
SELECT sum(u) FROM (SELECT int_div(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k);

-- HashTable for uint64 (used until (1<<11) elements), hence 2048 elements
SELECT 'uint64';
SET max_memory_usage = 2000000;
SELECT sum(u) FROM (SELECT int_div(number, 2048) AS k, uniqCombined(16)(reinterpret_as_string(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 4915200;
SELECT sum(u) FROM (SELECT int_div(number, 2048) AS k, uniqCombined(16)(reinterpret_as_string(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k);

SELECT 'K=18';

-- HashTable for uint32 (used until (1<<14) elements), hence 16384 elements
SELECT 'uint32';
SET max_memory_usage = 8000000;
SELECT sum(u) FROM (SELECT int_div(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 19660800;
SELECT sum(u) FROM (SELECT int_div(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k);

-- HashTable for uint64 (used until (1<<13) elements), hence 8192 elements
SELECT 'uint64';
SET max_memory_usage = 8000000;
SELECT sum(u) FROM (SELECT int_div(number, 8192) AS k, uniqCombined(18)(reinterpret_as_string(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k); -- { serverError 241 }
SET max_memory_usage = 19660800;
SELECT sum(u) FROM (SELECT int_div(number, 8192) AS k, uniqCombined(18)(reinterpret_as_string(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k);
