SELECT to_type_name((1,)), (1,);

EXPLAIN SYNTAX SELECT (1,);

DROP STREAM IF EXISTS tuple_values;

create stream tuple_values (t tuple(int)) ;

INSERT INTO tuple_values VALUES ((1)), ((2,));

DROP STREAM tuple_values;
