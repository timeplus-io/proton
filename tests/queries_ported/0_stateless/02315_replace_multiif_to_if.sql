EXPLAIN SYNTAX SELECT multi_if(number = 0, NULL, to_nullable(number)) FROM numbers(10000);
EXPLAIN SYNTAX SELECT CASE WHEN number = 0 THEN NULL ELSE to_nullable(number) END FROM numbers(10000);
