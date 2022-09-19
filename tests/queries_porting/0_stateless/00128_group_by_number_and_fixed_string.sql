SELECT n, k FROM (SELECT number AS n, to_fixed_string(materialize('   '), 3) AS k FROM system.numbers LIMIT 100000) GROUP BY n, k ORDER BY n DESC, k LIMIT 10;
