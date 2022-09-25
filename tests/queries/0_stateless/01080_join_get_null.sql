DROP STREAM IF EXISTS test_joinGet;

create stream test_joinGet(user_id nullable(int32), name string) Engine = Join(ANY, LEFT, user_id);

INSERT INTO test_joinGet VALUES (2, 'a'), (6, 'b'), (10, 'c'), (null, 'd');

SELECT to_nullable(to_int32(2)) user_id WHERE joinGet(test_joinGet, 'name', user_id) != '';

-- If the JOIN keys are nullable fields, the rows where at least one of the keys has the value NULL are not joined.
SELECT cast(null AS nullable(int32)) user_id WHERE joinGet(test_joinGet, 'name', user_id) != '';

DROP STREAM test_joinGet;
