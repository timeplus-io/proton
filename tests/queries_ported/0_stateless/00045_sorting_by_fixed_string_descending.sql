SELECT s FROM (SELECT to_fixed_string(materialize('abc'), 3) AS s FROM system.numbers LIMIT 100) ORDER BY s DESC
