SET join_algorithm = 'auto';
SET max_bytes_in_join = 100;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS nr;

CREATE STREAM t (`x` uint32, `s` low_cardinality(string)) ENGINE = Memory;
CREATE STREAM nr (`x` nullable(uint32), `s` nullable(string)) ENGINE = Memory;

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

DROP STREAM t;
DROP STREAM nr;
