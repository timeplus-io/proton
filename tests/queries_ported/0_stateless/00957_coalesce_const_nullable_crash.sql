SELECT coalesce(toNullable(1), toNullable(2)) as x, toTypeName(x);
SELECT coalesce(NULL, toNullable(2)) as x, toTypeName(x);
SELECT coalesce(toNullable(1), NULL) as x, toTypeName(x);
SELECT coalesce(NULL, NULL) as x, toTypeName(x);

SELECT coalesce(toNullable(materialize(1)), toNullable(materialize(2))) as x, toTypeName(x);
SELECT coalesce(NULL, toNullable(materialize(2))) as x, toTypeName(x);
SELECT coalesce(toNullable(materialize(1)), NULL) as x, toTypeName(x);
SELECT coalesce(materialize(NULL), materialize(NULL)) as x, toTypeName(x);

SELECT coalesce(to_low_cardinality(to_nullable(1)), to_low_cardinality(to_nullable(2))) as x, to_type_name(x);
SELECT coalesce(NULL, to_low_cardinality(to_nullable(2))) as x, to_type_name(x);
SELECT coalesce(to_low_cardinality(to_nullable(1)), NULL) as x, to_type_name(x);
