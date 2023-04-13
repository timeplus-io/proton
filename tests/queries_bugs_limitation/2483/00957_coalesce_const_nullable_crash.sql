SELECT coalesce(to_nullable(1), to_nullable(2)) as x, to_type_name(x);
SELECT coalesce(NULL, to_nullable(2)) as x, to_type_name(x);
SELECT coalesce(to_nullable(1), NULL) as x, to_type_name(x);
SELECT coalesce(NULL, NULL) as x, to_type_name(x);

SELECT coalesce(to_nullable(materialize(1)), to_nullable(materialize(2))) as x, to_type_name(x);
SELECT coalesce(NULL, to_nullable(materialize(2))) as x, to_type_name(x);
SELECT coalesce(to_nullable(materialize(1)), NULL) as x, to_type_name(x);
SELECT coalesce(materialize(NULL), materialize(NULL)) as x, to_type_name(x);

SELECT coalesce(to_low_cardinality(to_nullable(1)), to_low_cardinality(to_nullable(2))) as x, to_type_name(x);
SELECT coalesce(NULL, to_low_cardinality(to_nullable(2))) as x, to_type_name(x);
SELECT coalesce(to_low_cardinality(to_nullable(1)), NULL) as x, to_type_name(x);
