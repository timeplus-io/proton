SET cast_keep_nullable = 0;

SELECT CAST(toNullable(to_int32(0)) AS int32) as x, to_type_name(x);
SELECT CAST(toNullable(to_int8(0)) AS int32) as x, to_type_name(x);

SET cast_keep_nullable = 1;

SELECT CAST(toNullable(to_int32(1)) AS int32) as x, to_type_name(x);
SELECT CAST(toNullable(to_int8(1)) AS int32) as x, to_type_name(x);

SELECT CAST(toNullable(to_float32(2)), 'Float32') as x, to_type_name(x);
SELECT CAST(toNullable(to_float32(2)), 'uint8') as x, to_type_name(x);

SELECT CAST(if(1 = 1, toNullable(to_int8(3)), NULL) AS int32) as x, to_type_name(x);
SELECT CAST(if(1 = 0, toNullable(to_int8(3)), NULL) AS int32) as x, to_type_name(x);

SELECT CAST(a, 'int32') as x, to_type_name(x) FROM (SELECT materialize(CAST(42, 'Nullable(uint8)')) AS a);
SELECT CAST(a, 'int32') as x, to_type_name(x) FROM (SELECT materialize(CAST(NULL, 'Nullable(uint8)')) AS a);
