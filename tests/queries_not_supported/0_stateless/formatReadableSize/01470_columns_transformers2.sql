DROP STREAM IF EXISTS columns_transformers;

create stream columns_transformers (i int, j int, k int, a_bytes int, b_bytes int, c_bytes int) Engine=TinyLog;
INSERT INTO columns_transformers VALUES (100, 10, 324, 120, 8, 23);
SELECT  * EXCEPT 'bytes', COLUMNS('bytes') APPLY formatReadableSize FROM columns_transformers;

DROP STREAM IF EXISTS columns_transformers;

SELECT * APPLY x->arg_max(x, number) FROM numbers(1);
EXPLAIN SYNTAX SELECT * APPLY x->arg_max(x, number) FROM numbers(1);
