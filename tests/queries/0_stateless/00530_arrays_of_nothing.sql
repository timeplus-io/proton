SELECT [[[[],[]]]];
SELECT [[1], []];
SELECT [[[[],['']]]];
SELECT concat([], ['Hello'], []);
SELECT arrayPushBack([], 1), arrayPushFront([[]], []);

DROP STREAM IF EXISTS arr;
create stream arr (x array(string), y Nullable(string), z array(array(Nullable(string)))) ;

INSERT INTO arr SELECT [], NULL, [[], [NULL], [NULL, 'Hello']];
SELECT * FROM arr;

DROP STREAM arr;
