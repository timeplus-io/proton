SELECT is_nullable(3);
SELECT is_nullable(to_nullable(3));

SELECT is_nullable(NULL);
SELECT is_nullable(materialize(NULL));

SELECT is_nullable(to_low_cardinality(1));
SELECT is_nullable(to_nullable(to_low_cardinality(1)));

SELECT is_nullable(to_low_cardinality(materialize(1)));
SELECT is_nullable(to_nullable(to_low_cardinality(materialize(1))));
