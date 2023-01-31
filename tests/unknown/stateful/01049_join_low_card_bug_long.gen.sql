-- Tags: long

DROP STREAM IF EXISTS l;
DROP STREAM IF EXISTS r;
DROP STREAM IF EXISTS nl;
DROP STREAM IF EXISTS nr;
DROP STREAM IF EXISTS l_lc;
DROP STREAM IF EXISTS r_lc;

CREATE STREAM l (x uint32, lc string) ENGINE = Memory;
CREATE STREAM r (x uint32, lc string) ENGINE = Memory;
CREATE STREAM nl (x nullable(uint32), lc nullable(string)) ENGINE = Memory;
CREATE STREAM nr (x nullable(uint32), lc nullable(string)) ENGINE = Memory;
CREATE STREAM l_lc (x uint32, lc LowCardinality(string)) ENGINE = Memory;
CREATE STREAM r_lc (x uint32, lc LowCardinality(string)) ENGINE = Memory;

INSERT INTO r VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO nr VALUES (0, 'str'),  (1, 'str_r');
INSERT INTO r_lc VALUES (0, 'str'),  (1, 'str_r');

INSERT INTO l VALUES (0, 'str'), (2, 'str_l');
INSERT INTO nl VALUES (0, 'str'), (2, 'str_l');
INSERT INTO l_lc VALUES (0, 'str'), (2, 'str_l');

SELECT '-- join_algorithm = default, join_use_nulls = 0 --';

SET join_use_nulls = 0;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '-- join_algorithm = default, join_use_nulls = 1 --';

SET join_use_nulls = 1;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '-- join_algorithm = partial_merge, join_use_nulls = 0 --';

SET join_algorithm = 'partial_merge';SET join_use_nulls = 0;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '-- join_algorithm = partial_merge, join_use_nulls = 1 --';

SET join_algorithm = 'partial_merge';SET join_use_nulls = 1;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '-- join_algorithm = parallel_hash, join_use_nulls = 0 --';

SET join_algorithm = 'parallel_hash';SET join_use_nulls = 0;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '-- join_algorithm = parallel_hash, join_use_nulls = 1 --';

SET join_algorithm = 'parallel_hash';SET join_use_nulls = 1;


SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l RIGHT JOIN r USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l FULL JOIN r USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc RIGHT JOIN nr USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM l_lc FULL JOIN nr USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l RIGHT JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM l_lc AS l FULL JOIN nr AS r USING (lc) ORDER BY x, r.lc, l.lc;

--

SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl RIGHT JOIN r_lc USING (lc) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (x) ORDER BY x, lc;
SELECT lc, to_type_name(lc) FROM nl FULL JOIN r_lc USING (lc) ORDER BY x, lc;

SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l RIGHT JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (x) ORDER BY x, r.lc, l.lc;
SELECT to_type_name(r.lc), to_type_name(materialize(r.lc)), r.lc, materialize(r.lc), to_type_name(l.lc), to_type_name(materialize(l.lc)), l.lc, materialize(l.lc) FROM nl AS l FULL JOIN r_lc AS r USING (lc) ORDER BY x, r.lc, l.lc;

SELECT '--';

SET join_use_nulls = 0;

SELECT lc, to_type_name(lc)  FROM l_lc AS l RIGHT JOIN r_lc AS r USING (x) ORDER BY l.lc;

SELECT lowCardinalityKeys(lc.lc) FROM r FULL JOIN l_lc as lc USING (lc) ORDER BY lowCardinalityKeys(lc.lc);

SET join_algorithm = 'partial_merge';
SET join_use_nulls = 1;

SELECT r.lc, materialize(r.lc), to_type_name(r.lc), to_type_name(materialize(r.lc)) FROM l_lc AS l FULL OUTER JOIN r_lc AS r USING (x) ORDER BY r.lc;

DROP STREAM l;
DROP STREAM r;
DROP STREAM nl;
DROP STREAM nr;
DROP STREAM l_lc;
DROP STREAM r_lc;
