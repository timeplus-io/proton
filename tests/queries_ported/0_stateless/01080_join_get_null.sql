DROP STREAM IF EXISTS test_join_get;

CREATE STREAM test_join_get(user_id nullable(int32), name string) Engine = Join(ANY, LEFT, user_id);

INSERT INTO test_join_get VALUES (2, 'a'), (6, 'b'), (10, 'c'), (null, 'd');

SELECT to_nullable(to_int32(2)) as user_id WHERE join_get(test_join_get, 'name', user_id) != '';

-- If the JOIN keys are nullable fields, the rows where at least one of the keys has the value NULL are not joined.
SELECT cast(null AS nullable(int32)) as user_id WHERE join_get(test_join_get, 'name', user_id) != '';

DROP STREAM test_join_get;
