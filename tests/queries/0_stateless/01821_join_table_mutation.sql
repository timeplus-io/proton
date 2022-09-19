DROP STREAM IF EXISTS join_table_mutation;

create stream join_table_mutation(id int32, name string) ENGINE = Join(ANY, LEFT, id);

INSERT INTO join_table_mutation select number, to_string(number) from numbers(100);

SELECT count() FROM join_table_mutation;

SELECT name FROM join_table_mutation WHERE id = 10;

ALTER STREAM join_table_mutation DELETE WHERE id = 10;

SELECT count() FROM join_table_mutation;

SELECT name FROM join_table_mutation WHERE id = 10;

INSERT INTO join_table_mutation VALUES (10, 'm10');

SELECT name FROM join_table_mutation WHERE id = 10;

ALTER STREAM join_table_mutation DELETE WHERE id % 2 = 0;

ALTER STREAM join_table_mutation UPDATE name = 'some' WHERE 1; -- {serverError 48}

SELECT count() FROM join_table_mutation;

ALTER STREAM join_table_mutation DELETE WHERE name IN ('1', '2', '3', '4');

SELECT count() FROM join_table_mutation;

ALTER STREAM join_table_mutation DELETE WHERE 1;

SELECT count() FROM join_table_mutation;

DROP STREAM join_table_mutation;
