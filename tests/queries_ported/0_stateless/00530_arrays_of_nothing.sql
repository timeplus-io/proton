SELECT [[[[],[]]]];
SELECT [[1], []];
SELECT [[[[],['']]]];
SELECT concat([], ['Hello'], []);
SELECT array_push_back([], 1), array_push_front([[]], []);

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS arr;
create stream arr (x array(string), y nullable(string), z array(array(nullable(string)))) ;

INSERT INTO arr(x, y, z) SELECT [], NULL, [[], [NULL], [NULL, 'Hello']];
SELECT sleep(3);

SELECT * FROM arr;

DROP STREAM arr;
