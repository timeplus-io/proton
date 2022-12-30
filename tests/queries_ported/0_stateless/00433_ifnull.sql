SELECT if_null('x', 'y') AS res, to_type_name(res);
SELECT if_null(materialize('x'), materialize('y')) AS res, to_type_name(res);

SELECT if_null(to_nullable('x'), 'y') AS res, to_type_name(res);
SELECT if_null(to_nullable('x'), materialize('y')) AS res, to_type_name(res);

SELECT if_null('x', to_nullable('y')) AS res, to_type_name(res);
SELECT if_null(materialize('x'), to_nullable('y')) AS res, to_type_name(res);

SELECT if_null(to_nullable('x'), to_nullable('y')) AS res, to_type_name(res);

SELECT if_null(to_string(number), to_string(-number)) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT if_null(null_if(to_string(number), '1'), to_string(-number)) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT if_null(to_string(number), null_if(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
SELECT if_null(null_if(to_string(number), '1'), null_if(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;

SELECT if_null(NULL, 1) AS res, to_type_name(res);
SELECT if_null(1, NULL) AS res, to_type_name(res);
SELECT if_null(NULL, NULL) AS res, to_type_name(res);

SELECT if_null(null_if(to_string(number), '1'), null_if(to_string(-number), '-3')) AS res, to_type_name(res) FROM system.numbers LIMIT 5;
