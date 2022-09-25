SELECT great_circle_distance(0., 0., 0., 1.);
SELECT great_circle_distance(0., 89., 0, 90.);

SELECT geoDistance(0., 0., 0., 1.);
SELECT geoDistance(0., 89., 0., 90.);

SELECT great_circle_distance(0., 0., 90., 0.);
SELECT great_circle_distance(0., 0., 0., 90.);

SELECT geoDistance(0., 0., 90., 0.);
SELECT geoDistance(0., 0., 0., 90.);
