SET cast_keep_nullable = 0;

-- Value nullable
SELECT anyIf(CAST(number, 'Nullable(uint8)'), number = 3) AS a, to_type_name(a) FROM numbers(2);
-- Value and condition nullable
SELECT anyIf(number, number = 3) AS a, to_type_name(a) FROM (SELECT CAST(number, 'Nullable(uint8)') AS number FROM numbers(2));
-- Condition nullable
SELECT anyIf(CAST(number, 'uint8'), number = 3) AS a, to_type_name(a) FROM (SELECT CAST(number, 'Nullable(uint8)') AS number FROM numbers(2));
