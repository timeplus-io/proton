SELECT * FROM
(
    SELECT reinterpret_as_string(number + reinterpret_as_uint8('A')) AS k FROM system.numbers LIMIT 10
) as js1
ALL LEFT JOIN
(
    SELECT reinterpret_as_string(int_div(number, 2) + reinterpret_as_uint8('A')) AS k, number AS joined FROM system.numbers LIMIT 10
) as js2
USING k;
