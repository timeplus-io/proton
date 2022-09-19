SELECT to_uint64(round(exp10(number))) AS x, to_string(x) AS s FROM system.numbers LIMIT 10 FORMAT Pretty;
SELECT to_uint64(round(exp10(number))) AS x, to_string(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettyCompact;
SELECT to_uint64(round(exp10(number))) AS x, to_string(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettySpace;
SET max_block_size = 5;
SELECT to_uint64(round(exp10(number))) AS x, to_string(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettyCompactMonoBlock;
SELECT '\\''\'' FORMAT Pretty;
SELECT '\\''\'', 1 FORMAT Vertical;
