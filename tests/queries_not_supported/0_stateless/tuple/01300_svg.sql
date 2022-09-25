SELECT SVG((0., 0.));
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)]);
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]]);
SELECT SVG([[[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]]);
SELECT SVG((0., 0.), 'b');
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)], 'b');
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], 'b');
SELECT SVG([[[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]], 'b');

DROP STREAM IF EXISTS geo;
create stream geo (p tuple(float64, float64), s string, id int) engine=Memory();
INSERT INTO geo VALUES ((0., 0.), 'b', 1);
INSERT INTO geo VALUES ((1., 0.), 'c', 2);
INSERT INTO geo VALUES ((2., 0.), 'd', 3);
SELECT SVG(p) FROM geo ORDER BY id;
SELECT SVG(p, 'b') FROM geo ORDER BY id;
SELECT SVG((0., 0.), s) FROM geo ORDER BY id;
SELECT SVG(p, s) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(tuple(float64, float64)), s string, id int) engine=Memory();
INSERT INTO geo VALUES ([(0., 0.), (10, 0), (10, 10), (0, 10)], 'b', 1);
INSERT INTO geo VALUES ([(1., 0.), (10, 0), (10, 10), (0, 10)], 'c', 2);
INSERT INTO geo VALUES ([(2., 0.), (10, 0), (10, 10), (0, 10)], 'd', 3);
SELECT SVG(p) FROM geo ORDER BY id;
SELECT SVG(p, 'b') FROM geo ORDER BY id;
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)], s) FROM geo ORDER BY id;
SELECT SVG(p, s) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(array(tuple(float64, float64))), s string, id int) engine=Memory();
INSERT INTO geo VALUES ([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 'b', 1);
INSERT INTO geo VALUES ([[(1., 0.), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 'c', 2);
INSERT INTO geo VALUES ([[(2., 0.), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 'd', 3);
SELECT SVG(p) FROM geo ORDER BY id;
SELECT SVG(p, 'b') FROM geo ORDER BY id;
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], s) FROM geo ORDER BY id;
SELECT SVG(p, s) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(array(array(tuple(float64, float64)))), s string, id int) engine=Memory();
INSERT INTO geo VALUES ([[[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]], 'b', 1);
INSERT INTO geo VALUES ([[[(1., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]], 'c', 2);
INSERT INTO geo VALUES ([[[(2., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]], 'd', 3);
SELECT SVG(p) FROM geo ORDER BY id;
SELECT SVG(p, 'b') FROM geo ORDER BY id;
SELECT SVG([[[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]], [[(-10., -10.), (-10, -9), (-9, 10)]]], s) FROM geo ORDER BY id;
SELECT SVG(p, s) FROM geo ORDER BY id;

DROP STREAM geo;
