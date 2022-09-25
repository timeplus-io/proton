DROP STREAM IF EXISTS test_joinGet;

create stream test_joinGet(a string, b string, c float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_joinGet VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT joinGet(test_joinGet, 'c', 'ab', '1');

create stream test_lc(a low_cardinality(string), b low_cardinality(string), c float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_lc VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT joinGet(test_lc, 'c', 'ab', '1');

DROP STREAM test_joinGet;
DROP STREAM test_lc;
