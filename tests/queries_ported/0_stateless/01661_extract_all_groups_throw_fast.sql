SELECT repeat('abcdefghijklmnopqrstuvwxyz', number * 100) AS haystack, extract_all_groups_horizontal(haystack, '(\\w)') AS matches FROM numbers(1023); -- { serverError 128 }
SELECT count(extract_all_groups_horizontal(materialize('a'), '(a)')) FROM numbers(1000000) FORMAT Null; -- shouldn't fail
