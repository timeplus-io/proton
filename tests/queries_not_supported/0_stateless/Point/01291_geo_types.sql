DROP STREAM IF EXISTS geo;

SET allow_experimental_geo_types = 1;

CREATE STREAM geo (a Point, b Ring, c Polygon, d MultiPolygon) ENGINE=Memory();

INSERT INTO geo VALUES((0, 0), [(0, 0), (10, 0), (10, 10), (0, 10)], [[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]], [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);

SELECT * from geo;

DROP STREAM geo;
