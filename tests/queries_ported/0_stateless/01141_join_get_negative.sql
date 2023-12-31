DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (`s` string, `x` array(uint8), `k` uint64) ENGINE = Join(ANY, LEFT, k);
CREATE STREAM t2 (`s` string, `x` array(uint8), `k` uint64) ENGINE = Join(ANY, INNER, k);

SELECT join_get('t1', '', number) FROM numbers(2); -- { serverError 16 }
SELECT join_get('t2', 's', number) FROM numbers(2); -- { serverError 264 }

DROP STREAM t1;
DROP STREAM t2;
