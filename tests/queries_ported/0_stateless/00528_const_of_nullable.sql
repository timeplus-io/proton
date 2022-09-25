SELECT to_nullable(0) + 1 AS x, to_type_name(x), to_type_name(x);
SELECT to_nullable(materialize(0)) + 1 AS x, to_type_name(x), to_type_name(x);
SELECT materialize(to_nullable(0)) + 1 AS x, to_type_name(x), to_type_name(x);
SELECT to_nullable(0) + materialize(1) AS x, to_type_name(x), to_type_name(x);
SELECT to_nullable(materialize(0)) + materialize(1) AS x, to_type_name(x), to_type_name(x);
SELECT materialize(to_nullable(0)) + materialize(1) AS x, to_type_name(x), to_type_name(x);
