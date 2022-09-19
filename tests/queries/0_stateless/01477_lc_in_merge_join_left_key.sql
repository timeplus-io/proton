SET join_algorithm = 'auto';
SET max_bytes_in_join = 100;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS nr;

create stream t (`x` uint32, `s` LowCardinality(string)) ;
create stream nr (`x` Nullable(uint32), `s` Nullable(string)) ;

INSERT INTO t VALUES (1, 'l');
INSERT INTO nr VALUES (2, NULL);

SET join_use_nulls = 0;

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l LEFT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l RIGHT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l FULL JOIN t AS r USING (x) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l LEFT JOIN nr AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l RIGHT JOIN nr AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l FULL JOIN nr AS r USING (s) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l LEFT JOIN t AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l RIGHT JOIN t AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l FULL JOIN t AS r USING (s) ORDER BY t.x;

SET join_use_nulls = 1;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l LEFT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l RIGHT JOIN t AS r USING (x) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l FULL JOIN t AS r USING (x) ORDER BY t.x;

SELECT '-';

SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l LEFT JOIN nr AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l RIGHT JOIN nr AS r USING (s) ORDER BY t.x;
SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM t AS l FULL JOIN nr AS r USING (s) ORDER BY t.x;

SELECT '-';

-- TODO
-- SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l LEFT JOIN t AS r USING (s) ORDER BY t.x;
-- SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l RIGHT JOIN t AS r USING (s) ORDER BY t.x;
-- SELECT t.x, l.s, r.s, to_type_name(l.s), to_type_name(r.s) FROM nr AS l FULL JOIN t AS r USING (s) ORDER BY t.x;

DROP STREAM t;
DROP STREAM nr;
