-- Tags: shard

SET output_format_write_statistics = 0;
SELECT array_join(range(100)) AS x FROM remote('127.0.0.2', system.one) WHERE x GLOBAL IN (SELECT toUInt8(array_join(range(100)) + 50)) GROUP BY x ORDER BY x LIMIT 10 FORMAT JSONCompact;
SELECT array_join(range(100)) AS x FROM remote('127.0.0.{2,3}', system.one) WHERE x GLOBAL IN (SELECT toUInt8(array_join(range(100)) + 50)) GROUP BY x ORDER BY x LIMIT 10 FORMAT JSONCompact;
