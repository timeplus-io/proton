SET cast_keep_nullable = 0;

-- Value nullable
SELECT any_if(CAST(number, 'nullable(uint8)'), number = 3) AS a, to_type_name(a) FROM numbers(2);
-- Value and condition nullable
SELECT any_if(number, number = 3) AS a, to_type_name(a) FROM (SELECT CAST(number, 'nullable(uint8)') AS number FROM numbers(2));
-- Condition nullable
SELECT any_if(CAST(number, 'uint8'), number = 3) AS a, to_type_name(a) FROM (SELECT CAST(number, 'nullable(uint8)') AS number FROM numbers(2));
