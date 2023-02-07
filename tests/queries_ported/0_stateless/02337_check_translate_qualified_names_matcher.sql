CREATE STREAM nested_name_tuples
(
    `a` tuple(x string, y tuple(i int32, j string))
)
ENGINE = Memory;

INSERT INTO nested_name_tuples VALUES(('asd', (12, 'ddd')));

SELECT t.a.y.i FROM nested_name_tuples as t;
SELECT nested_name_tuples.a.y.i FROM nested_name_tuples as t;
