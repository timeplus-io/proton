DROP STREAM IF EXISTS test_joinGet;

CREATE STREAM test_joinGet(a string, b string, c float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_joinGet VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT join_get(test_joinGet, 'c', 'ab', '1');

CREATE STREAM test_lc(a low_cardinality(string), b low_cardinality(string), c float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_lc VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT join_get(test_lc, 'c', 'ab', '1');

DROP STREAM test_joinGet;
DROP STREAM test_lc;
