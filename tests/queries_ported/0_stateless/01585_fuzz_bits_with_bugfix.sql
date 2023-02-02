SELECT to_type_name(fuzz_bits('stringstring', 0.5)) from numbers(3);

SELECT to_type_name(fuzz_bits('stringstring', 0.5)) from ( SELECT 1 AS x UNION ALL SELECT NULL ) group by x
