SELECT wkt((0., 0.));
SELECT wkt([(0., 0.), (10., 0.), (10., 10.), (0., 10.)]);
SELECT wkt([[(0., 0.), (10., 0.), (10., 10.), (0., 10.)], [(4., 4.), (5., 4.), (5., 5.), (4., 5.)]]);
SELECT wkt([[[(0., 0.), (10., 0.), (10., 10.), (0., 10.)], [(4., 4.), (5., 4.), (5., 5.), (4., 5.)]], [[(-10., -10.), (-10., -9.), (-9., 10.)]]]);

DROP STREAM IF EXISTS geo;
create stream geo (p tuple(float64, float64), id int) engine=Memory();
INSERT INTO geo VALUES ((0, 0), 1);
INSERT INTO geo VALUES ((1, 0), 2);
INSERT INTO geo VALUES ((2, 0), 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(tuple(float64, float64)), id int) engine=Memory();
INSERT INTO geo VALUES ([(0, 0), (10, 0), (10, 10), (0, 10)], 1);
INSERT INTO geo VALUES ([(1, 0), (10, 0), (10, 10), (0, 10)], 2);
INSERT INTO geo VALUES ([(2, 0), (10, 0), (10, 10), (0, 10)], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(array(tuple(float64, float64))), id int) engine=Memory();
INSERT INTO geo VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 1);
INSERT INTO geo VALUES ([[(1, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 2);
INSERT INTO geo VALUES ([[(2, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP STREAM IF EXISTS geo;
create stream geo (p array(array(array(tuple(float64, float64)))), id int) engine=Memory();
INSERT INTO geo VALUES ([[[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 1);
INSERT INTO geo VALUES ([[[(1, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 2);
INSERT INTO geo VALUES ([[[(2, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP STREAM geo;
