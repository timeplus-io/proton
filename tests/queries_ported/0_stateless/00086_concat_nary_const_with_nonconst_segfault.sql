SELECT extract(to_string(number), '10000000') FROM system.numbers_mt WHERE concat(materialize('1'), '...', to_string(number)) LIKE '%10000000%' LIMIT 1
