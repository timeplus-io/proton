DROP STREAM IF EXISTS t1;

create stream t1 (`a` uint32, `b` uint32, `c` uint32 ) ;
INSERT INTO t1 VALUES (1, 1, 1), (1, 1, 2), (2, 2, 2), (1, 2, 2);

SELECT DISTINCT ON (a, b) a, b, c FROM t1;
SELECT DISTINCT ON (a, b) * FROM t1;
SELECT DISTINCT ON (a) * FROM t1;

-- fuzzer will fail, enable when fixed
-- SELECT DISTINCT ON (a, b) a, b, c FROM t1 LIMIT 1 BY a, b; -- { clientError 62 }

-- SELECT DISTINCT ON a, b a, b FROM t1; -- { clientError 62 }
-- SELECT DISTINCT ON a a, b FROM t1; -- { clientError 62 }

-- "Code: 47. DB::Exception: Missing columns: 'DISTINCT'" - error can be better
-- SELECT DISTINCT ON (a, b) DISTINCT a, b FROM t1; -- { serverError 47 }
-- SELECT DISTINCT DISTINCT ON (a, b) a, b FROM t1; -- { clientError 62 }

-- SELECT ALL DISTINCT ON (a, b) a, b FROM t1; -- { clientError 62 }
-- SELECT DISTINCT ON (a, b) ALL a, b FROM t1; -- { clientError 62 }

DROP STREAM IF EXISTS t1;
