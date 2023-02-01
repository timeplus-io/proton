DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS nr;

CREATE STREAM t (`x` uint32, `lc` low_cardinality(string)) ENGINE = Memory;
CREATE STREAM nr (`x` nullable(uint32), `lc` nullable(string)) ENGINE = Memory;

INSERT INTO t VALUES (1, 'l');
INSERT INTO nr VALUES (2, NULL);

SET join_use_nulls = 0;

SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY x;

SELECT '-';

SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc FROM t AS l RIGHT JOIN nr AS r USING (lc);

SELECT '-';

SET join_use_nulls = 1;

SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l LEFT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (x) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l FULL JOIN nr AS r USING (x) ORDER BY x;

SELECT '-';

SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, r.lc, to_type_name(r.lc) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l LEFT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x;
SELECT x, lc, materialize(r.lc) as y, to_type_name(y) FROM t AS l FULL JOIN nr AS r USING (lc) ORDER BY x;

SELECT '-';

SELECT x, lc FROM t AS l RIGHT JOIN nr AS r USING (lc);

SELECT '-';

DROP STREAM t;
DROP STREAM nr;
