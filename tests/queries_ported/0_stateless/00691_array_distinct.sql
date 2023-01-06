SELECT  array_distinct(array_map(x -> 0, range(2))) FROM numbers(2);

SELECT  array_distinct(materialize([[0], [0]])) FROM numbers(2);
SELECT  array_distinct(materialize(['', '', ''])) FROM numbers(2);
SELECT  array_distinct(materialize([0, 0, 0])) FROM numbers(2);
SELECT  array_distinct(materialize([0, 1, 1, 0])) FROM numbers(2);
SELECT  array_distinct(materialize(['', 'Hello', ''])) FROM numbers(2);


SELECT  array_distinct(materialize([[0], [0]])) FROM numbers(2);
SELECT  array_distinct(materialize(['', NULL, ''])) FROM numbers(2);
SELECT  array_distinct(materialize([0, NULL, 0])) FROM numbers(2);
SELECT  array_distinct(materialize([0, 1, NULL, 0])) FROM numbers(2);
SELECT  array_distinct(materialize(['', 'Hello', NULL])) FROM numbers(2);
