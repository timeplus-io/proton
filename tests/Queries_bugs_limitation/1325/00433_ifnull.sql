SELECT ifNull('x', 'y') AS res, to_type_name(res);
SELECT ifNull(materialize('x'), materialize('y')) AS res, to_type_name(res);

SELECT ifNull(toNullable('x'), 'y') AS res, to_type_name(res);
SELECT ifNull(toNullable('x'), materialize('y')) AS res, to_type_name(res);

SELECT ifNull('x', toNullable('y')) AS res, to_type_name(res);
SELECT ifNull(materialize('x'), toNullable('y')) AS res, to_type_name(res);

SELECT ifNull(toNullable('x'), toNullable('y')) AS res, to_type_name(res);

SELECT ifNull(to_string(number), to_string(-number)) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT ifNull(nullIf(to_string(number), '1'), to_string(-number)) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT ifNull(to_string(number), nullIf(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT ifNull(nullIf(to_string(number), '1'), nullIf(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;

SELECT ifNull(NULL, 1) AS res, to_type_name(res);
SELECT ifNull(1, NULL) AS res, to_type_name(res);
SELECT ifNull(NULL, NULL) AS res, to_type_name(res);

SELECT IFNULL(NULLIF(to_string(number), '1'), NULLIF(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
